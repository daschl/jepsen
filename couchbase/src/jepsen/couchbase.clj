(ns jepsen.couchbase
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [checker :as checker]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [tests :as tests]
             [util :as util :refer [timeout]]]
            [jepsen.os.debian :as debian]
            [knossos.model :as model])
  (:import (com.couchbase.client.java CouchbaseCluster Bucket ReplicaMode ReplicateTo)
           (java.util ArrayList)
           (com.couchbase.client.java.document JsonDocument)
           (com.couchbase.client.java.document.json JsonObject)
           (com.couchbase.client.core CouchbaseException)
           (clojure.lang ExceptionInfo)
           (com.couchbase.client.java.env DefaultCouchbaseEnvironment)
           (java.util.concurrent TimeoutException)))

; Works: write/read from primary
; Works: write/read; 1 replica, replicate to one, read from first replica
; Failed (expected): 3 replicas, replicate to one, read from replica first
; Failed (expected): 1 replica, no explicit wait for replication, read from first replica

(defn r   [_ _] {:type :invoke, :f :read})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn install!
  "Installs Couchbase on the given node."
  [node version]
  (when-not (.contains (or (debian/installed-version "couchbase-server") "") version)
    ;(debian/install ["python"])
    (c/su
      (debian/uninstall! ["couchbase-server"])
      (info node "Downloading and Installing Couchbase Server" version)
      (c/cd "/tmp"
            (c/exec :wget :-O (str "couchbase-server-enterprise_" version "-debian8_amd64.deb")
                    (str "https://packages.couchbase.com/releases/" version "/couchbase-server-enterprise_" version "-debian8_amd64.deb"))
            (c/exec :dpkg :-i (c/lit "couchbase-server-enterprise_*.deb")))

      ; Replace /usr/bin/asd with a wrapper that skews time a bit
      ;(c/exec :mv   "/usr/bin/asd" "/usr/local/bin/asd")
      (c/exec :echo
              "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" /usr/local/bin/asd" :> "/usr/bin/asd")
      (c/exec :chmod "0755" "/usr/bin/asd"))))



(defn start!
  "Starts Couchbase."
  [node test]
  (info node "starting couchbase")
  (c/su
    (c/exec :service :couchbase-server :start)))

(defn configure!
  "Configures Couchbase Server on the given node."
  [node test]


  )

(defn db
  "Couchbase Server for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "Setting Up Couchbase" version)
        (doto node
          (install! version)
          (start! test)
          (configure! test)))

    (teardown! [_ test node]
      (info node "tearing down Couchbase"))))


(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operations :f's which can
  safely be assumed to fail without altering the model state, and a body to
  evaluate. Catches errors and maps them to failure ops matching the
  invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]

     (try
       ~@body

       ; Timeouts could be either successful or failing
       (catch RuntimeException e#

         (case (instance? TimeoutException (.getCause e#))
           true (assoc ~op :type error-type#, :error :timeout))
       ))))

(defn fetch
  "Reads a record as a map of bin names to bin values from the given namespace,
  set, and key. Returns nil if no record found."
  [^Bucket client key]
  (.getInt (.content (.get (.getFromReplica client ^String key ReplicaMode/FIRST) 0))  "value")
  )

(defn close
  [^Bucket b]
  (.close b))

(defn put! [client key value]
  (.upsert client (JsonDocument/create key (.put (JsonObject/empty) "value", value) ) ReplicateTo/ONE)
  )

(defrecord CasRegisterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (.openBucket (CouchbaseCluster/create (.build (.mutationTokensEnabled (DefaultCouchbaseEnvironment/builder) true) )  (ArrayList. ["n1"])) "jepsen") ]
      (Thread/sleep 2000)
      (.upsert client (JsonDocument/create key (.put (JsonObject/empty) "value", 0) ))
      (assoc this :client client)))

  (invoke! [this test op]
    (with-errors op #{:read}
                 (case (:f op)

                   :read (assoc op
                           :type :ok,
                           :value (-> client (fetch key)))

                   :write (do (put! client key (get op :value))
                              (assoc op :type :ok))

                   )))

  (teardown! [this test]
    (close client)))

(defn cas-register-client
  "A basic CAS register on top of a single key and bin."
  []
  (CasRegisterClient. nil "jepsen" "cats" "mew"))


(defn couchbase-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "couchbase"
          :os debian/os
          :db (db "4.5.1")
          :client  (cas-register-client)
          :nemesis (nemesis/partition-random-node)
          :generator (->> (gen/mix [r w])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 15))
          :model   (model/cas-register 0)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear checker/linearizable})}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn couchbase-test})
                   (cli/serve-cmd))
            args)
  )