;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.content-store-test
  (:require
   [clojure.java.io :as io]
   [juxt.flow.protocols :as flow]
   [juxt.vext.flowable :as f]
   [juxt.vext.helpers :refer [h har]]
   [juxt.vext.content-store :as cs])
  (:import

   (io.vertx.reactivex.core Vertx)
   (io.vertx.core.file OpenOptions)
   (io.reactivex Flowable Single)
   (io.reactivex.processors MulticastProcessor)
   (io.vertx.reactivex.core.buffer Buffer)
   (org.reactivestreams Subscriber)
   (org.kocakosm.jblake2 Blake2b)))

;; This time, try to multicast the flowable

(def digest-encoder (java.util.Base64/getUrlEncoder))

;; TODO: This should return a Single, that when complete contains the digest and file info

;; TODO: Error handling

(let [ITEMS 10

      ITEM_SIZE 1024

      random-str (fn []
                   (->>
                    #(rand-nth (range (int \A) (inc (int \Z))))
                    (repeatedly)
                    (take ITEM_SIZE)
                    (map char)
                    (apply str)))

      buffers (doall
               (map
                (fn [s] (Buffer/buffer (.getBytes s)))
                (repeatedly ITEMS random-str)))

      ;; Go from buffers to iterable/array to static flowable
      flowable (Flowable/fromIterable buffers)

      vertx (Vertx/vertx)

      dir (io/file "/tmp/content-store")
      _ (.mkdirs dir)

      content-store (cs/->VertxFileContentStore vertx dir dir)
]

  (cs/store-content content-store flowable (fn [t] (println "Complete:" (pr-str t))))

  )
