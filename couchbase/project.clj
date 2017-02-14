(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen Couchbase Tests"
  :url "http://github.com/daschl/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.4"]
                 [com.couchbase.client/java-client "2.4.1"]]

  :main jepsen.couchbase
  )