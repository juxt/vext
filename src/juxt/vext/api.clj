;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.api
  (:require
   juxt.vext.flow
   [juxt.vext.helpers :refer [h]]
   [clojure.string :as string]
   [juxt.vext.header-names :refer [header-canonical-case]]
   [juxt.flow.protocols :as flow])
  (:import
   (io.vertx.core MultiMap)
   (io.vertx.core.net PemKeyCertOptions)
   (io.vertx.reactivex.core.buffer Buffer)
   (io.vertx.core.http HttpServerOptions)
   (io.vertx.reactivex.core.http HttpServer)))

(defn ^HttpServer run-http-server
  [ring-handler {:keys [vertx port] :as opts}]
  (let [server-options
        (..
         (new HttpServerOptions)
         #_(setPemKeyCertOptions
          (..
           (new PemKeyCertOptions)
           (setKeyPath "tls/key.pem")
           (setCertPath "tls/cert.pem")))
         #_(setSsl true)
         (setLogActivity (get opts :vertx/log-activity false)))
        server
        (..
         vertx
         (createHttpServer
          (..
           server-options
           (setPort port))))]
    (..
     server
     (requestHandler
      (h
       (fn [req]
         (let [ring-req
               {:server-port port
                :server-name (.getHost server-options)
                :remote-addr (str (.remoteAddress req))
                :uri (.path req)
                :query-string (.query req)
                :scheme (.scheme req)
                :request-method (keyword (string/lower-case (.rawMethod req)))
                :protocol (str (.version req))
                ;; This is aiming at Ring 2 lower-case request header
                ;; fields
                :headers (into {}
                               (for [[k v] (.entries (.headers req))]
                                 [(string/lower-case k) v]))

                ;; You need access to this for when there is no
                ;; alternative but to use a lower-level Vert.x API,
                ;; such as multipart uploads.
                :juxt.vext/request req ; low-level interface

                :juxt.vext/vertx vertx
                #_(->RingHeaders (.headers req))}]
           (ring-handler
            ring-req
            ;; Respond function
            (fn [{:keys [status headers body]}]
              ;; header-map here is only to allow us to preserve the
              ;; case of response headers, while potentially retrieving
              ;; them. Probably better to keep all as lower-case.
              (when (nil? status)
                (throw (ex-info "Status not set in response" {})))

              (let [header-map (cond-> (MultiMap/caseInsensitiveMultiMap)
                                 headers (.. (addAll headers)))
                    content-length (.get header-map "content-length")
                    _ (assert req)
                    _ (assert (.response req))
                    response
                    (..
                     req
                     response
                     (setChunked (not content-length))
                     ;;(setChunked false) ; while playing with SSE
                     (setStatusCode status))]

                #_(.closeHandler (.connection req)
                                 (->VertxHandler (fn [_]
                                                   (println "connection closed")
                                                   )))
                #_(println "connection" (.connection req))

                (doseq [[k v] headers]
                  ;; v can be a String, or Iterable<String>
                  (.putHeader response (header-canonical-case k) (str v)))

                ;; Now, make flowable

                ;; If the body is not a String, InputStream etc..


                ;; On Java 9 and above, body may return a
                ;; java.util.concurrent.Flow.Publisher.  Java 8 and below should
                ;; use org.reactivestreams.Publisher:
                ;; http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/Publisher.html?is-external=true

                ;; Body must satisfy protocol

                (cond

                  (instance? java.io.File body)
                  (. response sendFile (.getAbsolutePath body))

                  (satisfies? flow/Publisher body)
                  (flow/subscribe body (.toSubscriber response))

                  (and body (= (Class/forName "[B") (.getClass body)))
                  (.. response (write (Buffer/buffer body)) end)

                  ;; TODO: Support java.io.InputStream

                  :else
                  (cond-> response
                    body (.write body)
                    true (.end)
                    ))))

            (fn [e]
              (println "ERROR: " e)
              (..
               req
               response
               (setStatusCode 500)
               (setChunked true)
               (write "ERROR\n")
               (end))))))))

     listen)))

(defn handle-body
  "Receive the entire request body and pass as a buffer to the body-handler"
  [req body-handler]
  (assert (:juxt.vext/request req) "Request was not created by Vext")
  (.bodyHandler
   (:juxt.vext/request req)
   (h body-handler)))
