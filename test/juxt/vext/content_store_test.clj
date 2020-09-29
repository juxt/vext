;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.content-store-test
  (:require
   [clojure.test :refer [is deftest testing]]
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

(comment
  (content-hash-of-file
   (io/file "/tmp/content-store/AiyBwjmE_ya-cZFpVme7EjpVfmLFhEr8v3_gGTsuimCNQrpGdlvyMw==")))

(deftest content-store-test
  (let [ITEMS 10

        ITEM_SIZE 1024

        random-str
        (fn []
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

        content-store
        (cs/->VertxFileContentStore vertx dir dir)

        subscribe-result
        (fn [publisher p]
          (.subscribe
           publisher
           (reify io.reactivex.functions.Consumer
             (accept [_ v]
               (deliver p (assoc v :exists? (.exists (:file v))))))
           ;; Error handler
           (reify io.reactivex.functions.Consumer
             (accept [_ t]
               (deliver p {:error t})))))]

    (testing "Success"
      (let [p (promise)
            publisher (cs/post-content
                       content-store
                       flowable)]

        (is publisher)
        (subscribe-result publisher p)

        (let [result (deref p 1 {:error "Timeout!"})]
          (is (not (:error result)))
          (is (= #{:k :file :exists?} (set (keys result))))
          (is (:exists? result))
          (is (= (* ITEMS ITEM_SIZE) (.length (:file result))))
          (is (= (content-hash-of-file (:file result)) (:k result)))
          (when (.exists (:file result))
            (.delete (:file result))))))

    (testing "Failure due to a bad incoming buffer"
      (let [p (promise)
            publisher (cs/post-content
                       content-store
                       (Flowable/error (ex-info "Bad buffer" {})))]

        (is publisher)
        (subscribe-result publisher p)

        (let [result (deref p 1 {:error "Timeout!"})]
          (is (:error result))
          (is (= #{:error} (set (keys result))))
          (is (= "Bad buffer" (.getMessage (:error result)))))))

    (testing "Failure due to timeout"
      (let [p (promise)

            publisher
            (cs/post-content
             content-store
             (..
              flowable
              onBackpressureBuffer
              (delay 100 java.util.concurrent.TimeUnit/MILLISECONDS)))]

        (is publisher)
        (subscribe-result publisher p)

        (let [result (deref p 10 {:error "Timeout!"})]
          (is (:error result))
          (is (= #{:error} (set (keys result))))
          (is (= "Timeout!" (:error result))))))))
