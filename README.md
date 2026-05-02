# Big Data Processing and Applications 2026 - GROUP 12
## EXPLORATORY AND SENTIMENT ANALYSIS OF YELP REVIEWS USING APACHE SPARK

### Authors:
* ARSHMAN TARIQ
* MUHAMMAD FAIZAN TANVEER 

Online review platforms such as Yelp generate vast volumes of user-generated re
views that carry rich patterns about customer satisfaction, business quality, and con
sumer behaviour. The Yelp Open Dataset—a publicly available academic resource—
spans more than 8.5 GB of structured JSON data comprising roughly seven million re
views, 150,000 businesses, and two million user profiles across metropolitan areas in
the United States and Canada. Its scale, multi-table structure, and blend of textual and
numerical attributes make it suitable for Big Data analytics using Apache Spark.

This project applied Apache Spark to the full Yelp Open Dataset (>8.5 GB, ∼7M re
views) to conduct both EDA and ML-based sentiment classification, exceeding the
project requirement of a single analytical track. The EDA confirmed and extended
patterns reported in the literature: a strong positive rating skew, seasonal and di
urnal review rhythms, and semantically coherent sentiment vocabularies. The ML
pipeline—combining TF-IDF text features with 15 engineered meta-features in three
classifiers—produced interpretable models whose downstream business reputation
scores correlate with star ratings and actual business outcomes. Together, the analyses
demonstrate feature-engineered distributed computing for sentiment analysis.

