(ns cloudburst.core
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [com.amazonaws.services.lambda.runtime RequestStreamHandler]))

;; --------------------
;; AWS Handler
;; --------------------

(defn ^:private maybe-decode-json [res writer]
  (try
    (json/generate-stream res writer)
    (catch Exception e
      res)))

(defn handle-aws-request
  [handler in out ctx]
  (let [event (json/parse-stream (io/reader in) true)
        writer (io/writer out)]
    (-> event
        (handler ctx)
        (maybe-decode-json writer))
    (.flush writer)))

;; --------------------
;; Public functions
;; --------------------

(defn deployable-function? [var]
  (if (:cloudburst/deployable (meta var))
    true false))

(defn var->aws-lambda-fn-name [var]
  (let [sanitized-ns (-> var
                         meta
                         :ns
                         ns-name
                         (str/replace #"." "-"))
        fname (-> var meta :name)]
    (str sanitized-ns "-" fname)))

(defn sanitize-lambda-name [ns-sym fn-sym]
  (str (-> ns-sym str (str/replace #"\." "-")) "-" fn-sym))

(defn gen-meta
  ([fn-sym ns-proper]
   (let [{:keys [cloudburst/provider
                 cloudburst/runtime
                 cloudburst/memory
                 cloudburst/name] :as the-meta}
         (merge (or (meta ns-proper) {})
                (or (meta fn-sym) {}))]
     (-> the-meta
         (assoc :cloudburst/deployable true
                :cloudburst/name (or name (sanitize-lambda-name (ns-name ns-proper)
                                                                fn-sym))
                :cloudburst/provider (or provider :aws)
                :cloudburst/runtime (or runtime :java8)
                :cloudburst/memory (or memory 512))))))

;; --------------------
;; Macro utils
;; --------------------

(defn ^:private local-declare*
  [fname]
  `(declare ~fname))

(defn ^:private local-defn*
  [fname args body the-meta]
  `(def ~(vary-meta fname #(merge the-meta %))
     (fn ~args ~@body)))

(defn ^:private aws-defn*
  [fname args body _]
  (let [prefix (str fname "-")
        handle-request-method (symbol (str prefix "handleRequest"))
        class-name (str (ns-name *ns*) "." fname)]
    `(do
       (gen-class
        :name ~class-name
        :prefix ~prefix
        :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
       (defn ~handle-request-method
         [this# in# out# ctx#]
         (handle-aws-request ~fname in# out# ctx#)))))

;; --------------------
;; Main macro
;; --------------------

(defmacro defcloudfn
  [fname args & body]
  (assert (= (count args) 2) "Cloud function must have exactly three args [event context]")
  (let [the-meta (gen-meta fname *ns*)]
    `(do
       ~(local-declare* fname)
       ~(case (:cloudburst/provider the-meta)
          :aws (aws-defn* fname args body the-meta)
          nil)
       ~(local-defn* fname args body the-meta))))
