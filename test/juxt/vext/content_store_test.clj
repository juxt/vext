;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.content-store-test
  (:require
   [clojure.java.io :as io]
   [juxt.flow.protocols :as flow]
   [juxt.vext.flowable :as f]
   [juxt.vext.helpers :refer [h har]])
  (:import
   (io.vertx.reactivex.core Vertx)
   (io.vertx.core.file OpenOptions)
   (io.reactivex Flowable)
   (io.vertx.reactivex.core.buffer Buffer)
   (org.reactivestreams Subscriber)
   (org.kocakosm.jblake2 Blake2b)))


(def digest-encoder (java.util.Base64/getUrlEncoder))

;; Modelled on https://github.com/vert-x3/vertx-rx/tree/master/rx-java2-gen/src/main/java/io/vertx/reactivex/impl/WriteStreamSubscriberImpl.java

;; TODO: This should return a Single, that when complete contains the digest and file info

;; TODO: Can this be written more 'generically'? Separate the fact this is a
;; file from the content-hash - we could be writing to S3!

;; TODO: Error handling

(defn async-file-digest-subscriber
  "Return a org.reactivestreams.Subscriber that saves the stream to a file,
  computing a content-hash in-stream and using it as the filename."
  [afile]
  (let [BATCH_SIZE 16
        _ (.setWriteQueueMaxSize afile (long (/ BATCH_SIZE 2)))
        state (atom {:digest (new Blake2b 40)})
        request-more
        (fn []
          ;; TODO: Copy the logic in WriteStreamSubscriberImpl more completely.
          (let [subscription (:subscription @state)]
            (.request subscription BATCH_SIZE)))]
    (proxy [Subscriber] []
      (onSubscribe [subscription]
        (swap! state assoc :subscription subscription)
        ;; TODO: Register exceptionHandler
        (.drainHandler afile (h (fn [_] (request-more))))
        (request-more))

      (onNext [buf]
        ;; Asynchronously write bytes to the file
        (try
          (.write afile buf)
          (catch Throwable t
            (println "Cancelling subscription due to error" t)
            (try
              (.cancel (:subscription @state))
              (catch Throwable t
                (println "WARNING: Failed to cancel subscription, see error")
                (.onError this t)))
            (.onError this t)))

        (when (not (.writeQueueFull afile))
          (request-more))

        ;; Update blake2 digest
        (swap! state update :digest (fn [digest] (.update digest (.getBytes buf)))))

      (onError [t]
        (println "ERROR:" t))

      (onComplete []
        (.flush
         afile
         (har
          {:on-success
           (fn [_]
             (.close
              afile
              (har
               {:on-success
                (fn [_]
                  (let [filename
                        (.encodeToString encoder (.digest (:digest @state)))]
                    (println "TODO: Rename afile to " filename)))
                :on-error (fn [t] (println "Error closing" t))})))
           :on-error (fn [t] (println "Error flushing" t))}))))))

(time
 (let [ITEMS 1000

       ITEM_SIZE 1024

       vertx (Vertx/vertx)

       fs (.fileSystem vertx)

       tmpdir (doto (io/file "/tmp/mmxx.tmp") (.mkdirs))

       random-str (fn []
                    (->>
                     #(rand-nth (range (int \A) (inc (int \Z))))
                     (repeatedly)
                     (take ITEM_SIZE)
                     (map char)
                     (apply str)))

       buffers (map
                (fn [s] (Buffer/buffer (.getBytes s)))
                (repeatedly ITEMS random-str))

       ;; Go from buffers to iterable/array to static flowable
       flowable (Flowable/fromIterable buffers)

       afile (.openBlocking
              fs
              "/tmp/test.txt"
              (.. (new OpenOptions)
                  (setCreate true)
                  (setWrite true)))]

   (.subscribe
    flowable
    (async-file-digest-subscriber afile))))
