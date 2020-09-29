;; Copyright © 2020, JUXT LTD.

;; An asynchronous content-store which can take a (reactive) publisher of
;; buffers and save to a backing store.

(ns juxt.vext.content-store
  (:require
   [clojure.java.io :as io]
   [juxt.vext.flowable :as f])
  (:import
   (io.reactivex.processors MulticastProcessor AsyncProcessor)
   (io.vertx.core.file OpenOptions)
   (io.reactivex Single)
   (org.kocakosm.jblake2 Blake2b)))

(defprotocol ContentStore
  (get-content [_ k] "Get the content, as a publisher, mapped to the key, k")
  (post-content [_ ^org.reactivestreams.Publisher content-stream]
    "Store the content stream. Return a publisher which publishes a single map
    representing the result of the store, or an error. The key used is returned
    as the :k entry of this map."))

;; A Vertx based file storage implementation of the ContentStore protocol. The
;; implementation This returns a publisher which publishes the result. The
;; result contains the file and content-hash string used in its name.
(defrecord VertxFileContentStore [^io.vertx.reactivex.core.Vertx vertx
                                  ^java.io.File dest-dir
                                  ^java.io.File tmp-dir]
  ContentStore
  (get-content [_ k]
    nil
    )
  (post-content [_ content-stream]
    (let [tmp-file (io/file tmp-dir (str (java.util.UUID/randomUUID)))
          fs (.fileSystem vertx)
          afile (.openBlocking
                 fs
                 (.getAbsolutePath tmp-file)
                 (.. (new OpenOptions)
                     (setCreate true)
                     (setWrite true)))

          ;; Multicast the content stream to a temporary file and a content
          ;; hasher.
          multicaster (MulticastProcessor/create)

          ;; 1. Stream the content to the temporary file
          _ (.subscribe multicaster (.toSubscriber afile))

          content-hasher
          (->>
           multicaster

           (f/reduce
            (new Blake2b 40)
            #(.update %1 (.getBytes %2)))

           (f/map
            (fn [digest]
              (Single/just
               {:k (.encodeToString (java.util.Base64/getUrlEncoder) (.digest digest))})))

           (f/map
            (fn [{:keys [k] :as m}]
              ;; Rename the temporary file according to the hash
              (let [dest-file (io/file dest-dir k)]
                (.renameTo tmp-file dest-file)
                ;; Return a map indicating the results
                (Single/just (assoc m :file dest-file))))))

          ;; We'll publish the map computed by the content-hasher.  We can't
          ;; simply return the content-hasher for the caller to subscribe to,
          ;; because as soon as with subscribe the multicaster to the
          ;; content-stream, all the bytes will 'flow through'. So any new
          ;; subscribers to the content-hasher will get the initial state of the
          ;; content-hasher, rather than the state that contains the digest
          ;; updates from the bytes that have flowed through. That's not what we
          ;; want. So, we return an async processor, subscribed to the
          ;; content-hasher, which will act as a processor for the caller to
          ;; subscribe to. The async processor will 'hang on' to the final
          ;; result (or error), passing it to the caller's subscriber(s).
          publisher (AsyncProcessor/create)]

      ;; 2. Subscribe to the content-hasher (must be after 1 because we need to
      ;; move in lock-step, and our on-complete must be after the bytes have
      ;; been saved). Since the content-hasher is a Single (after composition),
      ;; we must turn it back into a Flowable.
      (.subscribe (.toFlowable content-hasher) publisher)

      ;; 3. Connect the processor to the upstream source (must be after 1 and 2
      ;; otherwise the stream is drained before the subscriptions get a chance
      ;; to receive and process the data).
      (.subscribe content-stream multicaster)

      ;; 4. Return the async-processor which the caller can subscribe on.
      publisher)))
