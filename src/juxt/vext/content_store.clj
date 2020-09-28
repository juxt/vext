;; Copyright © 2020, JUXT LTD.

;; An asynchronous

(ns juxt.vext.content-store
  (:require
   [clojure.java.io :as io]
   [juxt.vext.flowable :as f])
  (:import
   (io.reactivex.processors MulticastProcessor)
   (io.vertx.core.file OpenOptions)
   (io.reactivex Single)
   (org.kocakosm.jblake2 Blake2b)))

(defprotocol ContentStore
  (store-content [_ ^org.reactivestreams.Publisher content-stream on-complete]
    "Store the content stream. Call on-complete with results when done."))

(defrecord VertxFileContentStore [^io.vertx.reactivex.core.Vertx vertx
                                  ^java.io.File dest-dir
                                  ^java.io.File tmp-dir]
  ContentStore
  (store-content [_ content-stream on-complete]
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

          single
          (->>
           multicaster
           (f/reduce
            (new Blake2b 40)
            (fn [digest item]
              (.update digest (.getBytes item))))

           (f/map
            (fn [digest]
              (Single/just
               {:content-hash-str (.encodeToString (java.util.Base64/getUrlEncoder) (.digest digest))})))

           (f/map
            (fn [{:keys [content-hash-str] :as m}]
              ;; Rename the temporary file according to the hash
              (let [dest-file (io/file dest-dir content-hash-str)]
                (.renameTo tmp-file dest-file)
                (Single/just (assoc m :file dest-file))))))]

      ;; Stream the content to the temporary file
      (.subscribe multicaster (.toSubscriber afile))

      (.subscribe
       single
       (reify
         io.reactivex.functions.Consumer
         (accept [_ t]
           (on-complete t))))

      ;; Connect the processor to the upstream source
      (.subscribe content-stream multicaster))))
