;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.content-store-test
  (:require
   [clojure.test :refer [is deftest]]
   [clojure.java.io :as io]
   [juxt.vext.content-store :as cs])
  (:import
   (io.vertx.reactivex.core Vertx)
   (io.reactivex Flowable)
   (io.vertx.reactivex.core.buffer Buffer)
   (org.kocakosm.jblake2 Blake2b)))

(defn content-hash-of-file [f]
  (let [b (new Blake2b 40)]
    (.update
     b
     (.getBytes (slurp f)))
    (.encodeToString (java.util.Base64/getUrlEncoder) (.digest b))))

#_(content-hash-of-file
 (io/file "/tmp/content-store/AiyBwjmE_ya-cZFpVme7EjpVfmLFhEr8v3_gGTsuimCNQrpGdlvyMw=="))

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

    (let [p (promise)
          publisher (cs/store-content content-store flowable)]

      (is publisher)

      (.subscribe
       publisher
       (reify io.reactivex.functions.Consumer
         (accept [_ v]
           (deliver p (assoc v :exists? (.exists (:file v)))))))

      (let [result (deref p 1 {:error "Timeout!"})]
        (is (not (:error result)))
        (is (= #{:content-hash-str :file :exists?} (set (keys result))))
        (is (:exists? result))
        (is (= (* ITEMS ITEM_SIZE) (.length (:file result))))
        (is (= (content-hash-of-file (:file result)) (:content-hash-str result)))
        (when (.exists (:file result))
          (.delete (:file result)))))))
