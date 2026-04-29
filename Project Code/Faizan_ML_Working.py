# Generated from: Faizan_ML_Working.ipynb
# Converted at: 2026-04-29T12:16:00.288Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

# *The complete notebook from top to bottom runs in almost ~7.5 hours due to model training and processing (especially pandas and matplotlib).* 
# 
# 1. Instead, if you want to review the model evaluation, feature importance or Business Reputation Analysis, the model files and useful variables can be loaded from the "Model Loading" section directly to avoid preprocessing and model training. (It takes hardly 1 hr.)
# 2. And if you want to review the correlation matrix, data distribution, or retrain the model. Skip running all the cells and go to the "Feature Selection" section. Uncomment these lines to load df_full and run all the next cells directly.


# ### Spark Setup


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("BDPA-Project-Faizan-Working") \
    .getOrCreate()

# ### Reading JSONs


review_df = spark.read.json("yelp_academic_dataset_review.json")
business_df = spark.read.json("yelp_academic_dataset_business.json")
user_df = spark.read.json("yelp_academic_dataset_user.json")
checkin_df = spark.read.json("yelp_academic_dataset_checkin.json")
tip_df = spark.read.json("yelp_academic_dataset_tip.json")

# ### Reading Tables


# #### Table: Review


review_df.printSchema()

review_df.count()

review_df.show(5)

review_df.select('business_id','user_id','stars','date','useful','funny','cool').summary().toPandas()

# >> The columns: useful, funny and cool have invalid minimum values: -1. The valid value should be 0.


# #### Table: Business


business_df.printSchema()

business_df.count()

business_df.show(5)

business_df.select('business_id','review_count','hours','is_open','latitude','longitude','stars','state').summary().toPandas()

# #### Table: User


user_df.printSchema()

user_df.count()

user_df.show(5)

user_df.select('user_id','yelping_since','average_stars','review_count','cool','fans','funny','useful','compliment_cool','compliment_cute','compliment_funny','compliment_hot','compliment_list','compliment_more','compliment_note','compliment_photos','compliment_plain','compliment_profile','compliment_writer').summary().toPandas()

# #### Table: Checkin


checkin_df.printSchema()

checkin_df.count()

checkin_df.show(5)

checkin_df.summary().show()

# #### Table: Tip


tip_df.printSchema()

tip_df.count()

tip_df.show(5)

tip_df.select('business_id','user_id','compliment_count','date').summary().show()

# ### Dataframes


review_df.count()

# converting to the Pandas dataframe just for a better readability
review_df.limit(2).toPandas()

business_df.count()

business_df.limit(2).toPandas()

user_df.count()

user_df.limit(2).toPandas()

checkin_df.count()

checkin_df.limit(2).toPandas()

tip_df.count()

tip_df.limit(2).toPandas()

# ### Data Preprocessing


# #### Data Transformation


# > Converting data type of Date Column from String to Datetime


from pyspark.sql import functions as F

review_df = review_df.withColumn("date", F.to_timestamp("date"))

user_df.printSchema()

user_df = user_df.withColumn("yelping_since", F.to_timestamp("yelping_since"))

user_df.printSchema()

tip_df = tip_df.withColumn("date", F.to_timestamp("date"))

# > Transforming the column from String to Array


business_df = business_df.withColumn("categories", F.split(F.col("categories"), ", "))

business_df.printSchema()

business_df.limit(2).toPandas()

user_df = user_df.withColumn("friends", F.split(F.col("friends"), ", "))
user_df = user_df.withColumn("elite", F.split(F.col("elite"), ", "))

user_df.printSchema()

user_df.limit(2).toPandas()

# #### Data Cleaning


# > Fixing the invalid values


cols_to_fix = ["cool", "funny", "useful"]

review_df.select(cols_to_fix).summary("min").show()

for col_name in cols_to_fix:
    review_df = review_df.withColumn(
        col_name, 
        F.when(F.col(col_name) == -1, 0).otherwise(F.col(col_name))
    )

review_df.select(cols_to_fix).summary("min").show()

# > Checking for the NULL values


business_df.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c).cast("string") == ""), c)).alias(c) 
    for c in business_df.columns
]).toPandas()

business_df.count()

# ##### NULL values will be handled this way:
# - If column is unnecessary, column will be dropped
# - If column is necessary and rows count is low compared to the total rows, rows will be dropped
# - If column is necessary and rows count is high  compared to the total rows, values will be imputated


# > Checking if any duplicate review exists


# Total count vs. unique count
total_count = review_df.count()
unique_count = review_df.select("review_id").distinct().count()
print(f"Total Reviews: {total_count}")
print(f"Unique Reviews: {unique_count}")

if total_count > unique_count:
    print(f"Duplicates found! {total_count - unique_count} repeated IDs.")
else:
    print("No duplicates. Every review_id is unique.")

review_df.select('business_id','user_id','stars','date','useful','funny','cool').summary().toPandas()

# #### Feature Engineering


# > Filling the missing values in column address by combining city and postal_code


business_df = business_df.withColumn(
    "address",
    F.when(
        (F.col("address").isNull()) | (F.col("address") == ""), 
        F.concat_ws(", ", F.col("postal_code"), F.col("city"))
    ).otherwise(F.col("address"))
)

business_df.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c).cast("string") == ""), c)).alias(c) 
    for c in business_df.columns
]).toPandas()

# > Data Aggregation


checkin_df.limit(2).toPandas()

checkin_agg = checkin_df.withColumn(
    "checkin_count", 
    F.size(F.split(F.col("date"), ", "))
).select("business_id", "checkin_count")

checkin_agg.show(5)

business_tip_agg = tip_df.groupBy("business_id") \
    .agg(F.count("*").alias("business_tip_count"))

business_tip_agg.show(5)

user_tip_agg = tip_df.groupBy("user_id") \
    .agg(F.count("*").alias("user_tip_count"))

user_tip_agg.show(5)

# > Joining the tables


df = review_df \
    .join(business_df.withColumnRenamed("review_count", "business_review_count") \
                .withColumnRenamed("name", "business_name") \
                .withColumnRenamed("stars", "business_stars") \
                .withColumnRenamed("useful", "business_useful") \
                .withColumnRenamed("funny", "business_funny") \
                .withColumnRenamed("cool", "business_cool"),"business_id", "left") \
    .join(user_df.withColumnRenamed("review_count", "user_review_count") \
                .withColumnRenamed("name", "user_name") \
                .withColumnRenamed("stars", "user_stars")\
                .withColumnRenamed("useful", "user_useful") \
                .withColumnRenamed("funny", "user_funny") \
                .withColumnRenamed("cool", "user_cool"), "user_id", "left") \
    .join(checkin_agg, "business_id", "left") \
    .join(user_tip_agg, "user_id", "left") \
    .join(business_tip_agg, "business_id", "left")

# > Checking NULL and NaN


from pyspark.sql.types import DoubleType, FloatType

# Count NULLs for all, but only NaNs for numeric columns
null_nan_counts = df.select([
    F.count(
        F.when(
            F.col(c).isNull() | 
            (F.isnan(F.col(c)) if isinstance(df.schema[c].dataType, (DoubleType, FloatType)) else F.lit(False)), 
            c
        )
    ).alias(c) 
    for c in df.columns
])

null_nan_counts.show()

# Select only columns with missing values and convert to Pandas
null_nan_counts.select(
    'attributes', 'categories', 'hours', 'average_stars', 'compliment_cool', 
    'compliment_cute', 'compliment_funny', 'compliment_hot', 'compliment_list', 
    'compliment_more', 'compliment_note', 'compliment_photos', 'compliment_plain', 
    'compliment_profile', 'compliment_writer', 'user_cool', 'elite', 'fans', 
    'friends', 'user_funny', 'user_name', 'user_review_count', 'user_useful', 
    'yelping_since', 'checkin_count', 'user_tip_count', 'business_tip_count'
).toPandas()

# > Filling the NaN values


df = df.fillna({
    "checkin_count": 0,
    "user_tip_count": 0,
    "business_review_count": 0,
    "user_review_count": 0,
    "fans": 0,
    "text": "",
    "average_stars": 0,
    "business_stars": 0,
    "is_open": 0
})

df = df.withColumn(
    "categories",
    F.when(F.col("categories").isNull(), F.array().cast("array<string>"))
     .otherwise(F.col("categories"))
)

df.printSchema()

df.count()

df.limit(2).toPandas()

# > Review-based features


df = df.withColumn("review_length", F.length("text"))
df = df.withColumn("num_exclamations", F.size(F.split("text", "!")) - 1)
df = df.withColumn("num_caps", F.length(F.regexp_replace("text", "[^A-Z]", "")))

# > Correlation heatmap between numerical features


import pandas as pd
import matplotlib.pyplot as plt

num_cols = ["stars", "user_useful", "user_funny", "user_cool", "review_length", "user_review_count","fans"]
sample_pd = df.select(num_cols).sample(0.02, seed=42).toPandas()

corr_matrix = sample_pd.corr()

fig, ax = plt.subplots(figsize=(7, 6))
im = ax.imshow(corr_matrix.values, cmap="RdYlGn", vmin=-1, vmax=1)
ax.set_xticks(range(len(num_cols))); ax.set_yticks(range(len(num_cols)))
ax.set_xticklabels(num_cols, rotation=45, ha="right")
ax.set_yticklabels(num_cols)
for i in range(len(num_cols)):
    for j in range(len(num_cols)):
        ax.text(j, i, f"{corr_matrix.values[i, j]:.2f}",
                ha="center", va="center", fontsize=10,
                color="black" if abs(corr_matrix.values[i, j]) < 0.6 else "white")
plt.colorbar(im, ax=ax, label="Pearson Correlation")
ax.set_title("Correlation Matrix — Review Numerical Features (2% sample)", fontsize=12)
plt.tight_layout()
plt.show()

# > User features


df = df.withColumn("user_activity", F.col("user_review_count") + F.col("fans"))
df = df.withColumn("user_engagement", F.col("user_useful") + F.col("user_funny") + F.col("user_cool"))

# > Temporal features


df = df.withColumn("year", F.year("date"))
df = df.withColumn("month", F.month("date"))

# > Label Encoding


df = df.withColumn(
    "label",
    F.when(F.col("stars") <= 2, 0)
     .when(F.col("stars") == 3, 1)
     .otherwise(2)
)

# #### Feature Selection


df_full = df

df_full.write.mode("overwrite").parquet("processed_data.parquet")

# > Data Reduction


# If you want to review the correlation matrix, data distribution, or to retrain the model. 
# Skip running the above cells. Uncomment these lines to load df_full and run all the next cells directly. 

# df_full = spark.read.parquet("processed_data.parquet")
# df = df_full

df = df.select(
    "text", "label",
    "review_length", "num_exclamations", "num_caps",
    "average_stars", "user_review_count", "fans",
    "user_activity", "user_engagement",
    "business_stars","business_id",
    "business_review_count",
    "is_open", "categories", "state",
    "checkin_count", "user_tip_count",
    "year", "month"
)

df.limit(2).toPandas()

df.printSchema()

num_cols = ["label","review_length","num_exclamations","num_caps","average_stars","user_review_count","fans","user_activity","user_engagement","business_stars","business_review_count","is_open","checkin_count","user_tip_count","year","month"]
sample_pd = df.select(num_cols).sample(0.02, seed=42).toPandas()

corr_matrix = sample_pd.corr()

fig, ax = plt.subplots(figsize=(11, 9))
im = ax.imshow(corr_matrix.values, cmap="RdYlGn", vmin=-1, vmax=1)
ax.set_xticks(range(len(num_cols))); ax.set_yticks(range(len(num_cols)))
ax.set_xticklabels(num_cols, rotation=45, ha="right")
ax.set_yticklabels(num_cols)
for i in range(len(num_cols)):
    for j in range(len(num_cols)):
        ax.text(j, i, f"{corr_matrix.values[i, j]:.2f}",
                ha="center", va="center", fontsize=10,
                color="black" if abs(corr_matrix.values[i, j]) < 0.6 else "white")
plt.colorbar(im, ax=ax, label="Pearson Correlation")
ax.set_title("Correlation Matrix — Selected Features (2% sample)", fontsize=12)
plt.tight_layout()
plt.show()

# ## ML Pipeline


from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ### Handling NULL values


numeric_cols = [
    "review_length", "num_exclamations", "num_caps",
    "average_stars", "user_review_count", "fans",
    "user_activity", "user_engagement",
    "business_stars", "business_review_count",
    "is_open", "checkin_count", "user_tip_count",
    "year", "month"
]

df = df.fillna(0, subset=numeric_cols)

# Fix categories NULL (VERY IMPORTANT)
df = df.withColumn(
    "categories",
    F.when(F.col("categories").isNull(), F.array().cast("array<string>"))
     .otherwise(F.col("categories"))
)

# ### Data Resampling


# ── Class distribution analysis ───────────────────────────────────────────
label_counts = df.groupBy("label").count().orderBy("label").toPandas()
total = label_counts["count"].sum()
label_counts["percentage"] = (label_counts["count"] / total * 100).round(2)
label_counts["label_name"] = label_counts["label"].map({0: "Negative", 1: "Neutral", 2: "Positive"})
print(label_counts.to_string(index=False))

import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(8, 5))
colors = ["#e74c3c", "#f39c12", "#2ecc71"]
bars = ax.bar(label_counts["label_name"], label_counts["count"], color=colors, edgecolor="black")
for bar, pct in zip(bars, label_counts["percentage"]):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 15000,
            f"{pct}%", ha="center", fontsize=11, fontweight="bold")
ax.set_title("Sentiment Class Distribution (Before Balancing)", fontsize=14)
ax.set_xlabel("Sentiment Class")
ax.set_ylabel("Number of Reviews")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
plt.tight_layout()
plt.show()

# ── Stratified undersampling to fix class imbalance ──────────────────────
# Step 1: Separate classes
neg_df     = df.filter(F.col("label") == 0)
neutral_df = df.filter(F.col("label") == 1)
pos_df     = df.filter(F.col("label") == 2)

print(f"Before balancing → Negative: {neg_df.count():,}, Neutral: {neutral_df.count():,}, Positive: {pos_df.count():,}")

# Step 2: Undersample positive to match the minority class (neutral)
# We target neutral class size as the anchor
neutral_count = neutral_df.count()

# Fraction needed to downsample positive reviews
pos_fraction = min(1.0, neutral_count / pos_df.count())
neg_fraction = min(1.0, neutral_count / neg_df.count())

neg_sampled     = neg_df.sample(fraction=neg_fraction, seed=42)
neutral_sampled = neutral_df  # keep all neutral
pos_sampled     = pos_df.sample(fraction=pos_fraction, seed=42)

# Step 3: Combine balanced dataset
balanced_df = neg_sampled.union(neutral_sampled).union(pos_sampled)

# Verify
balanced_counts = balanced_df.groupBy("label").count().orderBy("label").toPandas()
print("\nAfter balancing:")
print(balanced_counts.to_string(index=False))

# ### Data Splitting


# Step 4: Split into train/test AFTER balancing
train_df, test_df = balanced_df.randomSplit([0.8, 0.2], seed=25)

# Step 5: Apply 25% sample for memory constraints on Rahti
train_df = train_df.sample(0.25, seed=25)
test_df  = test_df.sample(0.25, seed=25)

print(f"\nTrain size: {train_df.count():,} | Test size: {test_df.count():,}")

# ### Feature Pipeline


tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")

# Reduced size → faster
text = CountVectorizer(inputCol="filtered", outputCol="rawFeatures", vocabSize=2000)
idf = IDF(inputCol="rawFeatures", outputCol="text_features", minDocFreq=5)
# minDocFreq=5: words appearing in fewer than 5 documents are almost certainly noise or typos. 

cat = CountVectorizer(inputCol="categories", outputCol="category_features", vocabSize=200, minDF=3.0)
# minDF=3.0: category tags appearing in fewer than 3 businesses are dropped.

feature_cols = [
    "text_features", "category_features",
    "review_length", "num_exclamations", "num_caps",
    "average_stars", "user_review_count", "fans",
    "user_activity", "user_engagement",
    "business_stars", "business_review_count",
    "is_open", "checkin_count", "user_tip_count",
    "year", "month"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=False)  # withMean=False for sparse vectors (TF-IDF is sparse)

feature_pipeline = Pipeline(stages=[
    tokenizer, remover, text, idf,
    cat,
    assembler, scaler
])

# #### Compute class weights (For LR model)


# Even after undersampling, a small residual imbalance may remain.
# weightCol approach: assign each row a weight inversely proportional to class freq.

label_counts_train = train_df.groupBy("label").count().collect()
total_train = sum(r["count"] for r in label_counts_train)
n_classes = 3

class_weights = {r["label"]: total_train / (n_classes * r["count"]) for r in label_counts_train}
print("Class weights:", class_weights)

# Add weight column to both splits
from pyspark.sql.functions import when

def add_weights(sdf):
    return sdf.withColumn("classWeight",
        when(F.col("label") == 0, class_weights[0])
       .when(F.col("label") == 1, class_weights[1])
       .otherwise(class_weights[2])
    )

train_df = add_weights(train_df)
test_df  = add_weights(test_df)

# ### Feature Training


feature_model = feature_pipeline.fit(train_df)

train_featurized = feature_model.transform(train_df).cache()
test_featurized = feature_model.transform(test_df).cache()

train_featurized.count()  # force caches

# #### Input Size (For MLP)


input_size = train_featurized.select("features").first()[0].size
print("Feature size:", input_size)

mlp_train = train_featurized.sample(0.25, seed=42).cache()
mlp_test  = test_featurized.sample(0.25, seed=42).cache()
print(f"MLP train size: {mlp_train.count():,} | MLP test size: {mlp_test.count():,}")

# #### Model Parameters


lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    weightCol="classWeight",   # ← apply class weights
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.0
)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    maxDepth=15,
    minInstancesPerNode=10,
    featureSubsetStrategy="sqrt"
)

mlp = MultilayerPerceptronClassifier(
    featuresCol="features",
    labelCol="label",
    layers=[input_size, 64, 3],
    maxIter=100,
    blockSize=512,
    seed=42,
    solver="l-bfgs"
)

# #### Model Training


model_lr = lr.fit(train_featurized)

model_rf = rf.fit(train_featurized)

model_mlp = mlp.fit(mlp_train)

# #### Model Saving


feature_model.write().overwrite().save("feature_pipeline")

model_lr.write().overwrite().save("lr_model")
model_rf.write().overwrite().save("rf_model")
model_mlp.write().overwrite().save("mlp_model")

# #### Model Loading


from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

feature_model = PipelineModel.load("feature_pipeline")
lr_model = LogisticRegressionModel.load("lr_model")
rf_model = RandomForestClassificationModel.load("rf_model")
mlp_model = MultilayerPerceptronClassificationModel.load("mlp_model")
df_full = spark.read.parquet("processed_data.parquet") # For business reputation analysis at the end

# #### Model Prediction


pred_lr = lr_model.transform(test_featurized)

pred_rf = rf_model.transform(test_featurized)

pred_mlp = mlp_model.transform(mlp_test)

# ### Model Evaluation


evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

def evaluate_model(name, pred):
    acc = evaluator.evaluate(pred, {evaluator.metricName: "accuracy"})
    f1 = evaluator.evaluate(pred, {evaluator.metricName: "f1"})
    precision = evaluator.evaluate(pred, {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(pred, {evaluator.metricName: "weightedRecall"})
    
    print(f"\n{'='*40}")
    print(f" {name} — Overall Results")
    print(f"{'='*40}")
    print(f"Accuracy: {acc:.4f}")
    print(f"F1 Score: {f1:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")

    # Per-class breakdown
    label_map = {0.0: "Negative", 1.0: "Neutral", 2.0: "Positive"}
    pred_pd = pred.select("label", "prediction").toPandas()
    print(f"\n  Per-class Recall:")
    for label_val, label_name in label_map.items():
        class_df   = pred_pd[pred_pd["label"] == label_val]
        if len(class_df) == 0:
            continue
        correct    = (class_df["prediction"] == label_val).sum()
        class_recall = correct / len(class_df)
        print(f"    {label_name}: {class_recall:.4f} ({correct}/{len(class_df)})")

evaluate_model("LR", pred_lr)

evaluate_model("RF", pred_rf)

evaluate_model("MLP", pred_mlp) 

results = []
for name, pred in [("Logistic Regression", pred_lr), ("Random Forest", pred_rf), ("MLP", pred_mlp)]:
    acc       = evaluator.evaluate(pred, {evaluator.metricName: "accuracy"})
    f1        = evaluator.evaluate(pred, {evaluator.metricName: "f1"})
    precision = evaluator.evaluate(pred, {evaluator.metricName: "weightedPrecision"})
    recall    = evaluator.evaluate(pred, {evaluator.metricName: "weightedRecall"})
    results.append({"Model": name, "Accuracy": round(acc, 4),
                    "F1": round(f1, 4), "Precision": round(precision, 4), "Recall": round(recall, 4)})

summary_df = pd.DataFrame(results)
print(summary_df.to_string(index=False))

# #### Confusion Matrix


def plot_confusion_matrix(pred, model_name):
    labels = [0, 1, 2]
    label_names = ["Negative", "Neutral", "Positive"]

    cm = pred.groupBy("label", "prediction").count()
    full_cm = spark.createDataFrame(
        [(l, p) for l in labels for p in labels], ["label", "prediction"]
    ).join(cm, ["label", "prediction"], "left").fillna(0)

    cm_pd = full_cm.toPandas().pivot(index="label", columns="prediction", values="count").fillna(0)

    # Normalise rows for percentage display
    cm_norm = cm_pd.div(cm_pd.sum(axis=1), axis=0).round(3) * 100

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for ax, data, title, fmt in zip(
        axes,
        [cm_pd, cm_norm],
        [f"{model_name} — Count", f"{model_name} — Row %"],
        [".0f", ".1f"]
    ):
        im = ax.imshow(data.values, cmap="Blues")
        ax.set_xticks(range(3)); ax.set_yticks(range(3))
        ax.set_xticklabels(label_names); ax.set_yticklabels(label_names)
        ax.set_xlabel("Predicted"); ax.set_ylabel("Actual")
        ax.set_title(title)
        for i in range(3):
            for j in range(3):
                val = data.values[i, j]
                text = f"{val:{fmt}}" + ("%" if "%" in title else "")
                ax.text(j, i, text, ha="center", va="center",
                        color="white" if val > data.values.max() * 0.5 else "black",
                        fontsize=10, fontweight="bold")
        plt.colorbar(im, ax=ax)

    plt.tight_layout()
    plt.show()

plot_confusion_matrix(pred_lr,  "Logistic Regression")

plot_confusion_matrix(pred_rf,  "Random Forest")

plot_confusion_matrix(pred_mlp, "MLP")

# ### Model Interpretability


# ── RF and LR Feature Importance ─────────────────────────────────────────────────
import numpy as np

# Build feature name list (must match assembler inputCols order)
# TF-IDF → 2000 dims, CV → 200 dims, then the 15 scalar features

# Stage 2 is text_cv, Stage 4 is category cv
text_vocab = feature_model.stages[2].vocabulary
cat_vocab = feature_model.stages[4].vocabulary 

scalar_names = [
    "review_length", "num_exclamations", "num_caps",
    "average_stars", "user_review_count", "fans",
    "user_activity", "user_engagement",
    "business_stars", "business_review_count",
    "is_open", "checkin_count", "user_tip_count",
    "year", "month"
]

all_feature_names = text_vocab + cat_vocab + scalar_names

# Coefficients (weights) from the model: LR
coefficients = lr_model.coefficientMatrix.toArray()
importances = np.mean(np.abs(coefficients), axis=0)

# Coefficients (weights) from the model: RF
#importances = rf_model.featureImportances.toArray()

feat_df = pd.DataFrame({
    "feature": all_feature_names,
    "importance": importances
}).sort_values("importance", ascending=False)

# Show top 20 (skip the mostly-zero TF-IDF dims)
top_feat = feat_df[~feat_df["feature"].str.startswith("tf_")].head(30)
# Also get top 5 TF-IDF
top_tfidf = feat_df[feat_df["feature"].str.startswith("tf_")].head(5)
top_display = pd.concat([top_feat, top_tfidf]).sort_values("importance", ascending=False).head(25)

plt.figure(figsize=(10, 7))
plt.barh(top_display["feature"][::-1], top_display["importance"][::-1], color="#2980b9")
plt.xlabel("Feature Importance")
plt.title("Top Feature Importances (Logistic Regression)")
plt.tight_layout()
plt.show()

print("\nTop 15 Non-TF Features:")
print(feat_df[~feat_df["feature"].str.startswith("tf_")].head(15).to_string(index=False))

# #### Features important for sentiment


#Extract specific rows
neg_coeffs = coefficients[0] # Coefficients for Label 0 (Negative)
pos_coeffs = coefficients[2] # Coefficients for Label 2 (Positive)

def get_top_features(weights, names, top_n=20):
    return pd.DataFrame({
        "feature": names,
        "weight": weights
    }).sort_values("weight", ascending=False).head(top_n)

# 4. Get the features
top_positive_drivers = get_top_features(pos_coeffs, all_feature_names)
top_negative_drivers = get_top_features(neg_coeffs, all_feature_names)

print("Top 10 Drivers for POSITIVE Sentiment:")
print(top_positive_drivers[["feature", "weight"]].head(10))

print("\nTop 10 Drivers for NEGATIVE Sentiment:")
print(top_negative_drivers[["feature", "weight"]].head(10))

# ### Model Testing


samples_reviews = spark.createDataFrame([
    (
        "This place is absolutely amazing, loved everything!",
        51, 1, 0, # length, exclamations, caps
        4.5, 10, 2,
        12, 5,
        4.2, 100,
        1, ["None"], 0, 0, 2024, 5
    ),
    (
        "Worst experience ever. Completely disappointed.",
        47, 0, 0,
        2.0, 5, 0,
        5, 1,
        2.5, 50,
        1, ["None"], 0, 0, 2024, 5
    ),
    (
        "The food was average and the service was okay. Just an ordinary experience that was fine for the price.",
        103, 0, 0,
        3.0, 1, 0,
        1, 1,
        3.0, 20,
        1, ["None"], 0, 0, 2024, 5
    )
], [
    "text", "review_length", "num_exclamations", "num_caps",
    "average_stars", "user_review_count", "fans",
    "user_activity", "user_engagement",
    "business_stars", "business_review_count",
    "is_open", "categories",
    "checkin_count", "user_tip_count",
    "year", "month"
])

# Now run the combined dataframe through the pipeline
sample_features = feature_model.transform(samples_reviews)
pred = lr_model.transform(sample_features)

# Convert to Pandas and round the probabilities
results = pred.select("text", "prediction", "probability").toPandas()
results['probability'] = results['probability'].apply(lambda x: [round(float(p), 2) for p in x])

results

# ### Business Reputation Analysis


import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ── Step 1: Score all reviews using the saved LR model ────────────────────
# df_full

# ── Step 2: Apply feature pipeline to full df ─────────────────────────────
# Use the already-fitted feature_model (trained on balanced subset)
# to transform ALL 6.99M reviews for scoring.
df_scored = feature_model.transform(df_full)

# Apply LR model to get sentiment predictions on all reviews
df_scored = lr_model.transform(df_scored)

# Extract probability of positive sentiment (class 2) as a continuous score
# This gives us a soft sentiment score [0,1] rather than a hard label,
# which is more nuanced for business reputation tracking.
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def safe_pos_prob(v):
    try:
        val = float(v[2])
        return None if (val != val) else val  # NaN check: NaN != NaN is True
    except:
        return None

def safe_neg_prob(v):
    try:
        val = float(v[0])
        return None if (val != val) else val
    except:
        return None

get_pos_prob = udf(safe_pos_prob, FloatType())
get_neg_prob = udf(safe_neg_prob, FloatType())

df_scored = df_scored \
    .withColumn("pos_prob", get_pos_prob(F.col("probability"))) \
    .withColumn("neg_prob", get_neg_prob(F.col("probability"))) \
    .withColumn("sentiment_score",
        F.when(
            F.col("pos_prob").isNotNull() & F.col("neg_prob").isNotNull(),
            F.col("pos_prob") - F.col("neg_prob")
        ).otherwise(None)
    )

# Verify: count NaN/null rows
null_count = df_scored.filter(F.col("sentiment_score").isNull()).count()
total_count = df_scored.count()
print(f"Rows with valid sentiment score: {total_count - null_count:,} / {total_count:,}")
print(f"NaN/null rate: {null_count/total_count*100:.2f}%")
# sentiment_score ranges from -1 (very negative) to +1 (very positive)
# This is conceptually similar to the AFINN sentiment score used in
# Guda et al. (2022) "Sentiment Analysis: Predicting Yelp Scores".

# #### Calculating Business Reputation Score


# ── Step 3: Business Reputation Score ─────────────────────────────────────

# nanvl converts NaN to null so that F.avg() correctly ignores them
df_scored_clean = df_scored.withColumn(
    "sentiment_score_clean",
    F.nanvl(F.col("sentiment_score"), F.lit(None).cast("float"))
)

# Aggregate per business: mean sentiment score, review volume, % positive
biz_reputation = df_scored_clean.groupBy("business_id","categories").agg(
    F.count("*").alias("total_reviews"),
    F.avg("sentiment_score").alias("avg_sentiment_score"),
    F.avg("pos_prob").alias("avg_pos_prob"),
    F.avg("neg_prob").alias("avg_neg_prob"),
    F.avg("label").alias("avg_predicted_label"),
    F.sum(F.when(F.col("prediction") == 2, 1).otherwise(0)).alias("n_positive"),
    F.sum(F.when(F.col("prediction") == 0, 1).otherwise(0)).alias("n_negative"),
    F.first("business_stars").alias("avg_stars"),
    F.first("is_open").alias("is_open"),
    F.first("state").alias("state")
).withColumn(
    "positive_pct", F.round(F.col("n_positive") / F.col("total_reviews") * 100, 1)
).withColumn(
    "negative_pct", F.round(F.col("n_negative") / F.col("total_reviews") * 100, 1)
).filter(F.col("total_reviews") >= 20)  # minimum 20 reviews for reliability

# Cache for reuse
biz_reputation = biz_reputation.cache()
valid_scores = biz_reputation.filter(F.col("avg_sentiment_score").isNotNull()).count()
total_biz = biz_reputation.count()
print(f"Businesses with ≥20 reviews: {total_biz:,}")
print(f"Businesses with valid sentiment score: {valid_scores:,}")

# #### Correlation between Sentiment Scores and Star Rating


# ── Step 4: Sentiment Score vs Stars Correlation ───────────────────
# Key validation: Does our model's sentiment score correlate with actual stars?
# If it does, the model has learned genuine sentiment signal, not just noise.
# Guda et al. (2022) showed stars and sentiment score are highly correlated
# (r=0.52). We validate this on our model's output.

# Filter out null sentiment scores before correlation
biz_rep_valid = biz_reputation.filter(
    F.col("avg_sentiment_score").isNotNull()
)

corr_val = biz_rep_valid.select(
    F.corr("avg_sentiment_score", "avg_stars").alias("correlation")
).collect()[0]["correlation"]

print(f"Pearson correlation — Sentiment Score vs Actual Stars: {corr_val:.4f}")
print(f"Businesses used for correlation: {biz_rep_valid.count():,}")
print(f"Businesses excluded (NaN sentiment): {biz_reputation.count() - biz_rep_valid.count():,}")
# Expected: r > 0.5 confirms model validity

# Scatter plot: sentiment score vs actual stars
sample_biz = biz_rep_valid.filter(
    F.col("total_reviews").between(20, 500)
).sample(0.05, seed=42).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Plot 1: Sentiment Score vs Actual Stars
axes[0].scatter(sample_biz["avg_stars"], sample_biz["avg_sentiment_score"],
                alpha=0.3, s=10, color="#3498db")
axes[0].set_xlabel("Yelp Stars (Business Average)")
axes[0].set_ylabel("Model Sentiment Score (−1 to +1)")
axes[0].set_title(f"Sentiment Score vs Actual Stars\n(Pearson r = {corr_val:.3f})")
axes[0].axhline(0, color="red", linestyle="--", linewidth=0.8)

# Plot 2: Distribution of Reputation Scores
biz_pd = biz_rep_valid.select("avg_sentiment_score", "total_reviews").toPandas()
axes[1].hist(biz_pd["avg_sentiment_score"], bins=50, color="#2ecc71", edgecolor="black")
axes[1].set_xlabel("Average Sentiment Score")
axes[1].set_ylabel("Number of Businesses")
axes[1].set_title("Distribution of Business Reputation Scores")
axes[1].axvline(biz_pd["avg_sentiment_score"].mean(), color="red",
                linestyle="--", label=f"Mean={biz_pd['avg_sentiment_score'].mean():.2f}")
axes[1].legend()
plt.tight_layout()
plt.show()

# #### Top Businesses by Reputation


# ── Step 5: Top and Bottom Businesses by Reputation ───────────────────────
# Filter to businesses with substantial review volume for reliable ranking
well_reviewed = biz_rep_valid.filter(F.col("total_reviews") >= 100)

print("\n=== TOP 10 BUSINESSES BY SENTIMENT SCORE ===")
well_reviewed.orderBy("avg_sentiment_score", ascending=False) \
    .select("business_id", "categories","total_reviews", "avg_sentiment_score",
            "positive_pct", "avg_stars") \
    .limit(10).show(truncate=True)

print("\n=== BOTTOM 10 BUSINESSES BY SENTIMENT SCORE ===")
well_reviewed.orderBy("avg_sentiment_score", ascending=True) \
    .select("business_id", "categories","total_reviews", "avg_sentiment_score",
            "negative_pct", "avg_stars") \
    .limit(10).show(truncate=True)

# #### Temporal Sentiment Trend


# ── Step 6: Temporal Sentiment Trend ──────────────────────────────────────
# How has overall Yelp sentiment changed year by year (2005–2022)?
# This uses the 'year' column already in df_scored.
# Motivation: Sanchez (2021) noted Yelp ratings skew positive over time.
# We test whether our model's sentiment score shows the same trend.

yearly_sentiment = df_scored_clean.filter(
    F.col("sentiment_score_clean").isNotNull() &
    F.col("year").between(2005, 2022)
).groupBy("year").agg(
    F.avg("sentiment_score_clean").alias("avg_sentiment"),
    F.count("*").alias("review_count"),
    F.avg("pos_prob").alias("avg_pos_prob")
).filter(
    F.col("review_count") >= 500  # drop years with too few reviews for reliable average
).orderBy("year").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Plot 1: Sentiment score trend
color_bars = ["#e74c3c" if y in [2020, 2021] else "#3498db"
              for y in yearly_sentiment["year"]]
axes[0].bar(yearly_sentiment["year"], yearly_sentiment["avg_sentiment"],
            color=color_bars, edgecolor="black")
axes[0].set_xlabel("Year")
axes[0].set_ylabel("Average Sentiment Score (−1 to +1)")
axes[0].set_title("Average Review Sentiment Score by Year\n(Red = COVID years 2020–2021)")
axes[0].axhline(yearly_sentiment["avg_sentiment"].mean(), color="red",
                linestyle="--", linewidth=1, label="Overall mean")
axes[0].legend()

# Plot 2: Review volume + sentiment dual axis
ax2 = axes[1]
color_vol = "#95a5a6"
bars = ax2.bar(yearly_sentiment["year"], yearly_sentiment["review_count"] / 1e6,
               color=color_vol, label="Review Volume (M)", alpha=0.6)
ax2.set_ylabel("Review Volume (millions)")
ax2.set_xlabel("Year")
ax_twin = ax2.twinx()
ax_twin.plot(yearly_sentiment["year"], yearly_sentiment["avg_sentiment"],
             color="#e67e22", linewidth=2, marker="o", label="Avg Sentiment")
ax_twin.set_ylabel("Avg Sentiment Score")
ax2.set_title("Review Volume vs Sentiment Score by Year")
lines1, labels1 = ax2.get_legend_handles_labels()
lines2, labels2 = ax_twin.get_legend_handles_labels()
ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

for ax in [axes[0], ax2]: 
    ax.set_xticks(yearly_sentiment["year"][::2]) 
    ax.set_xticklabels(yearly_sentiment["year"][::2], rotation=45)
plt.tight_layout()
plt.show()

print(yearly_sentiment[["year", "review_count", "avg_sentiment"]].to_string(index=False))

# #### State-Level Sentiment Heatmap


# ── Step 7: State-Level Sentiment Heatmap ─────────────────────────────────
# Which US states show the most positive vs negative Yelp sentiment?
# Reuses 'state' column already in df.
state_sentiment = df_scored_clean.filter(
    F.col("state").isNotNull() &
    F.col("sentiment_score_clean").isNotNull()
).groupBy("state").agg(
    F.count("*").alias("review_count"),
    F.avg("sentiment_score").alias("avg_sentiment"),
    F.avg("pos_prob").alias("pct_positive")
).filter(F.col("review_count") >= 5000).orderBy("avg_sentiment", ascending=False)

state_pd = state_sentiment.toPandas()

print(f"States/provinces with ≥5000 reviews: {len(state_pd)}")
print(state_pd[["state", "review_count", "avg_sentiment"]].to_string(index=False))

fig, ax = plt.subplots(figsize=(12, max(6, len(state_pd) * 0.5)))
colors_state = ["#2ecc71" if s >= state_pd["avg_sentiment"].mean()
                    else "#e74c3c" for s in state_pd["avg_sentiment"]]
ax.barh(state_pd["state"][::-1], state_pd["avg_sentiment"][::-1],
        color=colors_state[::-1], edgecolor="black")
ax.axvline(0, color="black", linewidth=0.8)
ax.set_xlabel("Average Sentiment Score (−1 to +1)")
ax.set_title("Average Review Sentiment Score by State/Province\n(min. 1000 reviews)")
ax.set_xlim(-0.6, 0.6)
plt.tight_layout()
plt.show()

# #### Open vs Closed Business Sentiment


# ── Step 8: Open vs Closed Business Sentiment ─────────────────────────────
# Do businesses that are currently closed (is_open=0) have lower historical
# sentiment scores? This connects EDA (open/closed business analysis) to ML.
# Hypothesis: closed businesses likely accumulated more negative reviews,
# contributing to their closure.

open_closed = biz_reputation.filter(
    F.col("avg_sentiment_score").isNotNull() &
    F.col("is_open").isNotNull()
).groupBy("is_open").agg(
    F.count("*").alias("n_businesses"),
    F.avg("avg_sentiment_score").alias("mean_sentiment"),
    F.avg("avg_stars").alias("mean_stars"),
    F.avg("positive_pct").alias("mean_positive_pct")
).orderBy("is_open").toPandas()

print("\n=== OPEN vs CLOSED BUSINESS SENTIMENT ===")
print(open_closed.to_string(index=False))

if len(open_closed) == 2:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    labels_oc = ["Closed", "Open"]
    sent_vals = open_closed["mean_sentiment"].values
    star_vals = open_closed["mean_stars"].values
    colors_oc = ["#e74c3c", "#2ecc71"]

    # Plot 1: Sentiment score comparison
    bars = axes[0].bar(labels_oc, sent_vals, color=colors_oc, edgecolor="black", width=0.5)
    for bar, val in zip(bars, sent_vals):
        axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.003,
                f"{val:.3f}", ha="center", fontsize=12, fontweight="bold")
    axes[0].set_ylabel("Average Sentiment Score (−1 to +1)")
    axes[0].set_title("Sentiment Score: Open vs Closed Businesses")
    axes[0].set_ylim(0, max(sent_vals) * 1.2)

    # Plot 2: Actual star rating comparison (cross-validation)
    bars2 = axes[1].bar(labels_oc, star_vals, color=colors_oc, edgecolor="black", width=0.5)
    for bar, val in zip(bars2, star_vals):
        axes[1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f"{val:.2f}★", ha="center", fontsize=12, fontweight="bold")
    axes[1].set_ylabel("Mean Actual Star Rating")
    axes[1].set_title("Actual Star Rating: Open vs Closed Businesses\n(Cross-validation of sentiment findings)")
    axes[1].set_ylim(0, 5.5)

    plt.suptitle("Does Negative Sentiment Predict Business Closure?",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.show()