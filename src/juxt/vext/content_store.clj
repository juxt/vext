;; Copyright © 2020, JUXT LTD.

;; An asynchronous

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
  (store-content [_ ^org.reactivestreams.Publisher content-stream]
    "Store the content stream. Call on-complete with results when done."))

(defrecord VertxFileContentStore [^io.vertx.reactivex.core.Vertx vertx
                                  ^java.io.File dest-dir
                                  ^java.io.File tmp-dir]
  ContentStore
  (store-content [_ content-stream]
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
               {:content-hash-str (.encodeToString (java.util.Base64/getUrlEncoder) (.digest digest))})))

           (f/map
            (fn [{:keys [content-hash-str] :as m}]
              ;; Rename the temporary file according to the hash
              (let [dest-file (io/file dest-dir content-hash-str)]
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
