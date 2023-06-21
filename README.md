# Paper
This work is part of the paper **"A Comparative Study of Stream Reasoning Engines"**. You can find the full paper at [https://doi.org/10.1007/978-3-031-33455-9_2](https://doi.org/10.1007/978-3-031-33455-9_2).

RDFox Example Runner (Implements Unifying Benchmarking Interface)
===================
**Requirements:**
 * Java 7+
 * Maven

**License:**
 * The code is released under the Apache 2.0 license

**How to run**
1. Run **MainFiles.RDFoxRunningExample.java**

**The project has one main file:**
* **MainFiles.RDFoxRunningExample**:
   1. The runner connects to the configuration URL (http://localhost:11111/configuration.json) and parses the configuration 
   2. The runner registers the streams and queries that are specified in the configuration in the RDFox-Wrapper engine 
   3. The runner executes the continuous queries and saves the answers
   4. Whenever the streams do not continue sending RDF data, the runner publishes the answers (http://localhost:11112/answers.json)
