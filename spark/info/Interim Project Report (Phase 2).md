Interim Project Report (Phase 2\)  
Liam Greim and Logan Brenningmeyer

**Problem**: Slang is an important component of human language, providing snapshot-like insights into culture and communities at the time of their use. Our plan is to detect and chart the frequency of individual slang words over time using Reddit data from popular sub-reddits from the last two decades.

**Methodology:**

### Apache Spark

Spark serves as the backbone for data ingestion and the initial "Slang Detection" filtering phase. Spark’s ability to handle petabyte-scale data is essential for:

* Data Cleaning & Tokenization: We will use PySpark to ingest two decades of comments/posts, and handle the initial tokenization  
  * We will analyze an equal amount of data between the most popular subreddits and least popular subreddits to compare their usage of slang  
* Using Spark’s distributed architecture, we will run POS taggers across the corpus to identify the "ambiguously tagged" tokens that serve as our primary slang candidates  
* Spark will manage the heavy-duty join operations required to compare our Reddit candidates against the "Clean" corpus (e.g., Wikipedia or Project Gutenberg) to filter out standard English vocabulary.  
* Spark will aggregate the final "Slang Frequency" metrics, transforming raw model outputs into time-series data grouped by subreddit and timestamp.  
  * We can then track slang usage over time for ambiguously tagged tokens, as well as identify slang words based on rapid increases in frequency over time

### Ray:

While Spark handles the data, Ray provides the compute framework for the compute-intensive BERT workflows. Ray bridges the gap between raw data and deep learning:

* Distributed BERT Fine-Tuning (Ray Train): Fine-tuning a BERT model on millions of slang-heavy sentences is computationally prohibitive on a single machine. We will use Ray Train to distribute the PyTorch/Hugging Face training process across multiple GPU nodes, ensuring faster convergence and larger batch sizes.  
* Accelerated Inference (Ray Serve/Ray Data): Once the model is trained, we must "score" twenty years of data. Ray’s specialized handling of GPU tensors allows for significantly faster inference than standard Spark UDFs. We will use Ray to stream the preprocessed Spark DataFrames through our BERT model to classify tokens as slang or non-slang.  
* Ray Tune: To ensure our BERT model accurately distinguishes between typos and genuine slang, we will use Ray Tune to automate the search for optimal learning rates and dropout configurations.




**Distribution of Work:** Logan will work on the Spark portion, Liam will work on the Ray portion

