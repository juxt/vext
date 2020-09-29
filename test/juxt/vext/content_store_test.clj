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

(defn random-str [item-size]
  (->>
   #(rand-nth (range (int \A) (inc (int \Z))))
   (repeatedly item-size)
   (map char)
   (apply str)))

(defn make-random-flowable [n-items item-size]
  (Flowable/fromIterable
   ;; The doall here makes timings more
   ;; accurate, since much of the time is
   ;; spent on the creation of the random
   ;; flowables!
   (doall
    (map
     (fn [s] (Buffer/buffer (.getBytes s)))
     (repeatedly n-items #(random-str item-size))))))

(defn make-content-store []
  (let [vertx (Vertx/vertx)
        dir (io/file "/tmp/content-store")
        _ (.mkdirs dir)]
    (cs/->VertxFileContentStore vertx dir dir)))

(def ITEM_SIZE 1024)
(def ITEMS 10)

(deftest content-store-test
  (let [content-store (make-content-store)

        subscribe-result
        (fn [publisher p]
          (.subscribe
           publisher
           ;; onNext
           (reify io.reactivex.functions.Consumer
             (accept [_ v]
               (deliver p (assoc v :exists? (.exists (:file v))))))
           ;; onError
           (reify io.reactivex.functions.Consumer
             (accept [_ t]
               (deliver p {:error t})))))]

    (testing "Success"
      (let [p (promise)
            publisher (cs/post-content
                       content-store
                       (make-random-flowable ITEMS ITEM_SIZE))]

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
              (make-random-flowable ITEMS ITEM_SIZE)
              onBackpressureBuffer
              (delay 100 java.util.concurrent.TimeUnit/MILLISECONDS)))]

        (is publisher)
        (subscribe-result publisher p)

        (let [result (deref p 10 {:error "Timeout!"})]
          (is (:error result))
          (is (= #{:error} (set (keys result))))
          (is (= "Timeout!" (:error result))))))

    (testing "Multiple flowables"

      (let [p (promise)
            a (atom {:files []})
            flowable
            (Flowable/merge
             (doall
              (for [_ (range 10)]
                (cs/post-content
                 (make-content-store)
                 (make-random-flowable ITEMS ITEM_SIZE)))))]

        (.subscribe
         flowable
         ;; onNext
         (reify io.reactivex.functions.Consumer
           (accept [_ v]
             (println "End of file" v)
             (swap! a update :files conj v)))

         ;; onError
         (reify io.reactivex.functions.Consumer
           (accept [_ t]))

         ;; onComplete
         (reify io.reactivex.functions.Action
           (run [_]
             (deliver p @a))))

        (let [result (deref p 10 {:error "Timeout!"})]
          (is (= 10 (count (:files result)))))))))
