;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.content-store-test
  (:require
   [clojure.test :refer [is deftest]]
   [clojure.java.io :as io]
   [juxt.vext.content-store :as cs])
  (:import
   (io.vertx.reactivex.core Vertx)
   (io.reactivex Flowable)
   (io.vertx.reactivex.core.buffer Buffer)))

(deftest content-store-test
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

        content-store (cs/->VertxFileContentStore vertx dir dir)]

    (let [p (promise)]
      (cs/store-content content-store flowable (fn [v] (deliver p v)))
      (is (= #{:content-hash-str :file} (set (keys @p)))))))
