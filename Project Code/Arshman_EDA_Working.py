# Generated from: Arshman_EDA_Working.ipynb
# Converted at: 2026-04-29T12:15:29.626Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

# *This notebook takes ~35 minutes to run from top to bottom.*


# ### Spark Setup


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("BDPA-Project-Arshman-Working") \
    .getOrCreate()

# ### Reading JSONs


review_df = spark.read.json("yelp_academic_dataset_review.json")
business_df = spark.read.json("yelp_academic_dataset_business.json")
user_df = spark.read.json("yelp_academic_dataset_user.json")
checkin_df = spark.read.json("yelp_academic_dataset_checkin.json")
tip_df = spark.read.json("yelp_academic_dataset_tip.json")

# ### Reading Tables


# ##### Review Table


review_df.printSchema()

review_df.count()

review_df.show(5)

review_df.limit(2).toPandas()

#check unique values
# Returns an integer count
unique_count = review_df.select("useful","funny","cool").distinct().count()


# To see counts for 'useful' sorted by the number 1, 2, 3...
review_df.groupBy("useful").count().orderBy("useful", ascending=True).show()

# Repeat for the others
review_df.groupBy("funny").count().orderBy("funny", ascending=True).show()
review_df.groupBy("cool").count().orderBy("cool", ascending=True).show()


business_df.limit(2).toPandas()

business_df.printSchema()

business_df.count()

user_df.printSchema()

user_df.count()

checkin_df.printSchema()

tip_df.printSchema()

# ### Cleaning Dataset & Transformation


# #### Data Selection


# 1. Select only relevant columns (drop the rest early)
user_clean_df = user_df.select(
    "user_id",
    "name",
    "review_count",
    "average_stars",
    "fans",
    "useful",
    "funny",
    "cool",
    "yelping_since"
)



# #### Handling Missing and Invalid Values 


# Count Nulls
from pyspark.sql import functions as F
review_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in review_df.columns]).show()


# Check if any columns contain the string "null", "None", or are just empty spaces
# Find rows where the text is just spaces or totally empty
review_df.filter(F.trim(F.col("text")) == "").show()




cols_to_fix = ["cool", "funny", "useful"]

review_df.select(cols_to_fix).summary("min").show()

for col_name in cols_to_fix:
    review_df = review_df.withColumn(
        col_name, 
        F.when(F.col(col_name) == -1, 0).otherwise(F.col(col_name))
    )

review_df.select(cols_to_fix).summary("min").show()

# To see counts for 'useful' sorted by the number 1, 2, 3...
review_df.groupBy("useful").count().orderBy("useful", ascending=True).show()

# Repeat for the others
review_df.groupBy("funny").count().orderBy("funny", ascending=True).show()
review_df.groupBy("cool").count().orderBy("cool", ascending=True).show()


user_clean_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in user_clean_df.columns]).show()


from pyspark.sql import functions as F

# Filter out actual nulls, empty strings, or the literal string 'None'
user_clean_df = user_clean_df.filter(
    (F.col("name").isNotNull()) & 
    (F.col("name") != "") & 
    (F.col("name") != "None")
)

# Verify again
user_clean_df.select([
    F.count(F.when(F.isnan(c) | F.col(c).isNull() | (F.col(c) == ""), c)).alias(c) 
    for c in user_clean_df.columns
]).show()


checkin_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in checkin_df.columns]).show()

tip_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in tip_df.columns]).show()

# Removed isnan() to avoid the datatype mismatch error
business_df.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c).cast("string") == ""), c)).alias(c) 
    for c in business_df.columns
]).show()


# Look at rows where hours are null
# Shows only the Name and Categories for the 103 problematic rows
#business_df.filter(F.col("hours").isNull()).select("hours").show()


#business_df.filter(F.col("categories").isNotNull()).select("categories").show()


#business_df.filter(F.col("categories").isNull()).select("categories").show()

# Use dot notation to reach inside the attributes struct
#business_df.select("name", "attributes.DogsAllowed").show()


# #### Drop rows where 'categories' is null


# Filter out rows where categories is NULL OR an empty string
business_clean_df = business_df.filter(
    (F.col("categories").isNotNull()) & 
    (F.col("categories") != "")
)

# If you also want to catch 'NaN' (though rare for text columns):
business_clean_df = business_clean_df.filter(~F.isnan(F.col("categories")))

# Filter out rows with an empty string in postal_code
business_clean_df = business_clean_df.filter(F.col("postal_code") != "")

# #### Dropping Complex / Irrelevant Features i.e attributes & hours


#Drop column Hours and Attributes
business_clean_df = business_clean_df.drop("attributes", "hours")

business_clean_df.limit(2).toPandas()

#checking business table again with both null and nan
business_clean_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull() | (F.col(c).cast("string") == ""), c)).alias(c) for c in business_clean_df.columns]).show()

# #### Imputation/Concatenation


from pyspark.sql import functions as F

# Impute address by combining city and postal_code
business_clean_df = business_clean_df.withColumn(
    "address",
    F.when(
        (F.col("address").isNull()) | (F.col("address") == ""), 
        F.concat_ws(", ", F.col("city"), F.col("postal_code"))
    ).otherwise(F.col("address"))
)

# Final Verification
business_clean_df.select(
    F.count(F.when((F.col("address").isNull()) | (F.col("address") == ""), "address")).alias("empty_address_count")
).show()


# ### Statistics Section


business_clean_df.printSchema()

# Select the numerical columns and call summary for business table
business_stats_df = business_clean_df.select("stars", "review_count","latitude","longitude").summary()

business_stats_df.show()


# Select the numerical columns and call summary for reviews table
review_stats_df = review_df.select('stars','useful','funny','cool','date').summary()

review_stats_df.show()

# Select the numerical columns and call summary for users table
user_stats_df = user_clean_df.select('review_count','yelping_since','useful','funny','cool','fans').summary()

user_stats_df.show()

# Select the numerical columns and call summary for checkin table
checkin_stats_df = checkin_df.select('date').summary()

checkin_stats_df.show()

# Select the numerical columns and call summary for tip table
tip_stats_df = tip_df.select('date','compliment_count').summary()

tip_stats_df.show()

# #### Convert all date in string format to timestamp


review_df = review_df.withColumn("date", F.to_timestamp("date"))

user_clean_df = user_clean_df.withColumn("yelping_since", F.to_timestamp("yelping_since"))

#checkin_df = checkin_df.withColumn("date", F.to_timestamp("date"))

tip_df = tip_df.withColumn("date", F.to_timestamp("date"))

# ### Exploratory Data Analysis (EDA)


# #### Geographic Distribution of Yelp Businesses


sample_df = business_clean_df.select("latitude", "longitude")
pdf = sample_df.toPandas()

!pip install basemap basemap-data-hires


import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

plt.figure(figsize=(15,6))

# Shift center to North America (lat_0=45, lon_0=-100) to see labels better
m1 = Basemap(projection='ortho', lat_0=45, lon_0=-100)

m1.fillcontinents(color='#bbdaa4', lake_color='#4a80f5')
m1.drawmapboundary(fill_color='#4a80f5')
m1.drawcountries(linewidth=0.5, color="black")
m1.drawstates(linewidth=0.1, color="gray")

# --- ADDING REGION NAMES ---
# 1. Define places (Name, Latitude, Longitude)
labels = [
    ("NORTH AMERICA", 40, -100),
    ("EUROPE", 50, 15),
    ("ATLANTIC OCEAN", 25, -45)
]

# 2. Convert coordinates and plot text
for name, lat, lon in labels:
    x, y = m1(lon, lat) # Convert GPS to Map coordinates
    plt.text(x, y, name, fontsize=12, fontweight='bold', 
             ha='center', color='black', alpha=0.8)

# Use your business data
mxy = m1(pdf["longitude"].values, pdf["latitude"].values)
m1.scatter(mxy[0], mxy[1], s=2, c="orange", alpha=0.6)

plt.title("Geographic Distribution of Yelp Businesses")
plt.show()


# #### Heatmap of business Country/Provincial Level


!pip install folium


import folium
from folium.plugins import HeatMap

# 1. Sample the data (Heatmaps are slow with millions of points)
# We select lat/lon and convert to a list of lists for Folium
heat_data = business_clean_df.select("latitude", "longitude").toPandas()
heat_data_list = heat_data.values.tolist()

# 2. Initialize the map centered on a general location (e.g., USA)
m = folium.Map(location=[20.0, 0.0], zoom_start=2)

# 3. Add the HeatMap layer
HeatMap(heat_data_list).add_to(m)

# 4. Save or display
m.save("business_heatmap.html")
m # This will display the map in your notebook


# #### Heatmap of business City Level


import folium
from folium.plugins import HeatMap

# 1. Sample the data (Heatmaps are slow with millions of points)
# We select lat/lon and convert to a list of lists for Folium
heat_data = business_clean_df.select("latitude", "longitude").toPandas()
heat_data_list = heat_data.values.tolist()

# 2. Initialize the map centered on a general location (e.g., USA)
m = folium.Map(location=[39.8283, -98.5795], zoom_start=6)

# 3. Add the HeatMap layer
HeatMap(heat_data_list).add_to(m)

# 4. Save or display
m.save("business_heatmap_citylevel.html")
m # This will display the map in your notebook


# #### Top 10 cities with most businesses


# Grouping by city and counting all business_ids
top_10_cities = business_clean_df.groupBy("city") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10)

top_10_cities.show()


# #### Top 10 Categories


# Categories with maximum businesses
top_10_categories = business_clean_df.groupBy("categories") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10)

top_10_categories.show()

#The issue above is that there is reptition, like Restaurants, Pizza is one category, and  Pizza, Restaurants is another, 
#so i want to take split all words, and count them and put them into major catgeories

# 1. Transform the 'categories' string into an array, then into individual rows
# 2. Trim whitespace to ensure " Pizza" and "Pizza" are treated as the same word

all_category_words = business_clean_df.withColumn("category_word", F.explode(F.split(F.col("categories"), ","))) \
    .withColumn("category_word", F.trim(F.col("category_word")))

# 3. Group by the new individual words and count occurrences
word_counts = all_category_words.groupBy("category_word") \
    .count() \
    .orderBy("count", ascending=False)

# Show the results
#word_counts.show() # shows only top 20

# Convert the entire result to a Pandas DataFrame
full_category_list = word_counts.toPandas()

# This displays the whole thing in a scrollable format
import pandas as pd
pd.set_option('display.max_rows', None) # Force pandas to show every row
display(full_category_list)


# 2. Save it as a CSV file
full_category_list.to_csv("all_categories_list.csv", index=False)

print("Export complete! Look for 'all_categories_list.csv' in your folder.")

# 1. Define the High-Level Taxonomy Mapping
# This dictionary maps the specific words from your dataset to Major Categories.
# We map the most frequent ones to catch the vast majority of your data.
category_mapping = {
    # Food & Dining
    'Restaurants': 'Food & Dining', 'Food': 'Food & Dining', 'Nightlife': 'Food & Dining',
    'Bars': 'Food & Dining', 'Sandwiches': 'Food & Dining', 'American (Traditional)': 'Food & Dining',
    'Pizza': 'Food & Dining', 'Coffee & Tea': 'Food & Dining', 'Fast Food': 'Food & Dining',
    'Breakfast & Brunch': 'Food & Dining', 'American (New)': 'Food & Dining', 'Burgers': 'Food & Dining',
    'Mexican': 'Food & Dining', 'Italian': 'Food & Dining', 'Specialty Food': 'Food & Dining',
    'Seafood': 'Food & Dining', 'Desserts': 'Food & Dining', 'Chinese': 'Food & Dining',
    'Bakeries': 'Food & Dining', 'Salad': 'Food & Dining', 'Chicken Wings': 'Food & Dining',
    'Cafes': 'Food & Dining', 'Dumplings': 'Food & Dining', 'Pita': 'Food & Dining', 
    'Somali': 'Food & Dining', 'Hakka': 'Food & Dining', 'Cucina campana': 'Food & Dining',
    'Beer Hall': 'Food & Dining', 'Donburi': 'Food & Dining', 'Tonkatsu': 'Food & Dining',
    'Lahmacun': 'Food & Dining', 'Cheese Tasting Classes': 'Food & Dining',
    
    # Shopping & Retail
    'Shopping': 'Shopping & Retail', 'Home & Garden': 'Shopping & Retail', 'Fashion': 'Shopping & Retail',
    'Grocery': 'Shopping & Retail', 'Bike Shop': 'Shopping & Retail', 'Concept Shops': 'Shopping & Retail',
    'Ceremonial Clothing': 'Shopping & Retail', 'Props': 'Shopping & Retail',
    
    # Beauty & Spas
    'Beauty & Spas': 'Beauty & Spas', 'Hair Salons': 'Beauty & Spas', 'Nail Salons': 'Beauty & Spas',
    'Hair Removal': 'Beauty & Spas', 'Foot Care': 'Beauty & Spas',
    
    # Home & Local Services
    'Home Services': 'Home & Local Services', 'Local Services': 'Home & Local Services', 
    'Real Estate': 'Home & Local Services', 'Pets': 'Home & Local Services', 
    'Homeless Shelters': 'Home & Local Services', 'Fire Departments': 'Home & Local Services',
    'Veterans Organizations': 'Home & Local Services', 'Sauna Installation & Repair': 'Home & Local Services',
    'Water Suppliers': 'Home & Local Services', 'Natural Gas Suppliers': 'Home & Local Services',
    'Housing Cooperatives': 'Home & Local Services', 'Metal Detector Services': 'Home & Local Services',
    
    # Health & Medical
    'Health & Medical': 'Health & Medical', 'Doctors': 'Health & Medical', 'Nephrologists': 'Health & Medical',
    'Otologists': 'Health & Medical', 'Infectious Disease Specialists': 'Health & Medical',
    'Hospitalists': 'Health & Medical', 'Sperm Clinic': 'Health & Medical', 'Storefront Clinics': 'Health & Medical',
    'Osteopaths': 'Health & Medical', 'Animal Assisted Therapy': 'Health & Medical', 'Mohels': 'Health & Medical',
    'Faith-based Crisis Pregnancy Centers': 'Health & Medical',
    
    # Automotive
    'Automotive': 'Automotive', 'Auto Repair': 'Automotive',
    
    # Arts, Entertainment & Events
    'Arts & Entertainment': 'Arts, Entertainment & Events', 'Event Planning & Services': 'Arts, Entertainment & Events',
    'Carousels': 'Arts, Entertainment & Events', 'Art Consultants': 'Arts, Entertainment & Events',
    'Art Installation': 'Arts, Entertainment & Events', 'Rodeo': 'Arts, Entertainment & Events',
    'Experiences': 'Arts, Entertainment & Events', 'General Festivals': 'Arts, Entertainment & Events',
    'Silent Disco': 'Arts, Entertainment & Events', 'Outdoor Movies': 'Arts, Entertainment & Events',
    'Trade Fairs': 'Arts, Entertainment & Events', 'Karaoke Rental': 'Arts, Entertainment & Events',
    
    # Active Life & Fitness
    'Active Life': 'Active Life & Fitness', 'Fitness & Instruction': 'Active Life & Fitness',
    'Sledding': 'Active Life & Fitness', 'Bocce Ball': 'Active Life & Fitness', 'Ski Schools': 'Active Life & Fitness',
    'Kiteboarding': 'Active Life & Fitness', 'Bicycle Paths': 'Active Life & Fitness', 'Cheerleading': 'Active Life & Fitness',
    'Fencing Clubs': 'Active Life & Fitness', 'Bubble Soccer': 'Active Life & Fitness', 'Sport Equipment Hire': 'Active Life & Fitness',
    
    # Travel & Hospitality
    'Hotels & Travel': 'Travel & Hospitality', 'Hotels': 'Travel & Hospitality', 'Ferries': 'Travel & Hospitality',
    'Hotel bar': 'Travel & Hospitality',
    
    # Education & Professional Services
    'Professional Services': 'Education & Professional Services', 'Waldorf Schools': 'Education & Professional Services',
    'Vocal Coach': 'Education & Professional Services', 'Sports Psychologists': 'Education & Professional Services',
    'Kitchen Incubators': 'Education & Professional Services', 'Circus Schools': 'Education & Professional Services',
    'Calligraphy': 'Education & Professional Services'
}

# 1. Start building the case statement
# We initialize it with a 'dummy' value that we will overwrite
mapping_expr = F.lit("Other")

# 2. Loop through your dictionary and chain 'when' conditions
# We go in reverse or specific order so that if a business has multiple 
# categories, it picks the most relevant one from your list.
for category_key, major_group in category_mapping.items():
    mapping_expr = F.when(F.col("categories").contains(category_key), major_group).otherwise(mapping_expr)

# 3. Apply the mapping to create the new column
business_clean_df = business_clean_df.withColumn("main_category", mapping_expr)

# 4. Check the results
business_clean_df.select("name", "categories", "main_category").show(10, truncate=False)


# Categories with maximum businesses
top_10_categories = business_clean_df.groupBy("main_category") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10)

top_10_categories.show()

import matplotlib.pyplot as plt

# 1. Convert PySpark results to Pandas
pdf = top_10_categories.toPandas()

# 2. Sort so the highest is at the top for a horizontal bar chart
pdf = pdf.sort_values('count', ascending=True)

# 3. Create the plot
fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.barh(pdf['main_category'], pdf['count'], color='#58508d')

# 4. Make it "Beautiful" (Clean & Professional)
ax.spines['top'].set_visible(False)    # Remove top border
ax.spines['right'].set_visible(False)  # Remove right border
ax.spines['bottom'].set_visible(False) # Remove bottom axis
ax.xaxis.set_visible(False)            # Hide the numbers at the bottom

# 5. Add data labels at the end of each bar
ax.bar_label(bars, padding=5, fontsize=11, fontweight='bold', color='#333333')

# 6. Final Titles
plt.title("Top 10 Business Categories", fontsize=16, pad=20, loc='left', color='#222222')
plt.tight_layout()

plt.show()


# #### Top 10 Most reviewed Business Categories


#using pyspark method
from pyspark.sql.functions import desc

# Join tables, group by category, count reviews, and sort
top_reviewed_categories = review_df.join(business_clean_df, "business_id") \
    .groupBy("main_category") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

top_reviewed_categories.show()


#using pySQL method
# Create temporary views
review_df.createOrReplaceTempView("reviews")
business_clean_df.createOrReplaceTempView("businesses")

# Write the SQL query
top_reviewed_sql = spark.sql("""
    SELECT 
        b.main_category, 
        COUNT(r.review_id) as review_count
    FROM reviews r
    JOIN businesses b ON r.business_id = b.business_id
    GROUP BY b.main_category
    ORDER BY review_count DESC
    LIMIT 10
""")

top_reviewed_sql.show()


# 1. Convert PySpark results to Pandas
pdf = top_reviewed_categories.toPandas()

# 2. Sort so the highest is at the top for a horizontal bar chart
pdf = pdf.sort_values('count', ascending=True)

# 3. Create the plot
fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.barh(pdf['main_category'], pdf['count'], color='#50808d')

# 4. Make it "Beautiful" (Clean & Professional)
ax.spines['top'].set_visible(False)    # Remove top border
ax.spines['right'].set_visible(False)  # Remove right border
ax.spines['bottom'].set_visible(False) # Remove bottom axis
ax.xaxis.set_visible(False)            # Hide the numbers at the bottom


# 5. Add data labels formatted as Millions/Thousands
def format_label(value):
    if value >= 1_000_000:
        return f'{value/1_000_000:.1f}M'
    else:
        return f'{value/1_000:.0f}k'

labels = [format_label(val) for val in pdf['count']]
ax.bar_label(bars, labels=labels, padding=5, fontsize=11, fontweight='bold', color='#333333')


# 6. Final Titles
plt.title("Top 10 most reviewed Categories", fontsize=16, pad=20, loc='left', color='#222222')
plt.tight_layout()

plt.show()

business_clean_df.show(2)

# #### Cities having the highest-rated businesses versus the most reviews in Food & Dining Category


from pyspark.sql.functions import avg, sum, count, desc

city_stats = business_clean_df \
    .filter(business_clean_df.main_category == 'Food & Dining') \
    .groupBy("city") \
    .agg(
        count("business_id").alias("total_businesses"),
        avg("stars").alias("avg_rating"),
        sum("review_count").alias("total_reviews")
    ) \
    .orderBy(
        desc("total_businesses"), 
        desc("avg_rating"), 
        desc("total_reviews")
    )

city_stats.show()


# #### Cities having the highest-rated businesses versus the most reviews in Home & Local Services Category


city_stats = business_clean_df \
    .filter(business_clean_df.main_category == 'Home & Local Services') \
    .groupBy("city") \
    .agg(
        count("business_id").alias("total_businesses"),
        avg("stars").alias("avg_rating"),
        sum("review_count").alias("total_reviews")
    ) \
    .orderBy(
        desc("total_businesses"), 
        desc("avg_rating"), 
        desc("total_reviews")
    )

city_stats.show()


# #### Distribution of Stars(ratings)


review_df.select("stars").distinct().show()


review_df.groupBy("stars").count().orderBy("stars").show()

import matplotlib.pyplot as plt

# 1. Get unique star values and counts from PySpark
star_counts_pdf = review_df.groupBy("stars").count().orderBy("stars").toPandas()

# 2. Create the plot
plt.figure(figsize=(8, 4))
bars = plt.bar(star_counts_pdf['stars'], star_counts_pdf['count'], color='#58508d', width=0.4)

# 3. Beauty & Styling
plt.title("Distribution of Star Ratings", fontsize=16, pad=20, fontweight='bold')
plt.xlabel("Star Rating", fontsize=12)
plt.ylabel("Number of Reviews", fontsize=12)

# Remove the box/spines for a modern look
ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# 4. Add data labels on top of each bar (using the formatter we liked earlier)
labels = [f'{val/1e6:.1f}M' if val >= 1e6 else f'{val/1e3:.0f}k' for val in star_counts_pdf['count']]
ax.bar_label(bars, labels=labels, padding=3, fontsize=10, fontweight='bold')

plt.show()


import matplotlib.pyplot as plt

star_dist = star_counts_pdf
star_dist["label"] = star_dist["stars"].apply(lambda x: f"{int(x)}★")

total = star_dist["count"].sum()
star_dist["pct"] = (star_dist["count"] / total * 100).round(1)

colors = ["#c0392b", "#e67e22", "#f1c40f", "#2ecc71", "#27ae60"]
fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.bar(star_dist["label"], star_dist["count"], color=colors, edgecolor="black", width=0.6)
for bar, pct in zip(bars, star_dist["pct"]):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20000,
            f"{pct}%", ha="center", fontsize=11, fontweight="bold")
ax.set_title("Distribution of Star Ratings Across All Reviews", fontsize=14)
ax.set_xlabel("Star Rating")
ax.set_ylabel("Number of Reviews")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
plt.tight_layout()
plt.show()

print(star_dist[["stars", "count", "pct"]].to_string(index=False))

# #### Review length vs star rating


# Add review_length to review_df
review_df = review_df.withColumn("review_length", F.length(F.col("text")))

# Average review length per star rating
length_by_star = review_df.groupBy("stars") \
    .agg(
        F.avg("review_length").alias("avg_length"),
        F.percentile_approx("review_length", 0.5).alias("median_length")
    ) \
    .orderBy("stars") \
    .toPandas()

fig, ax = plt.subplots(figsize=(9, 5))
x = length_by_star["stars"]
ax.bar(x - 0.2, length_by_star["avg_length"], 0.4, label="Mean Length", color="#3498db", edgecolor="black")
ax.bar(x + 0.2, length_by_star["median_length"], 0.4, label="Median Length", color="#e67e22", edgecolor="black")
ax.set_title("Review Length by Star Rating", fontsize=14)
ax.set_xlabel("Star Rating")
ax.set_ylabel("Characters")
ax.set_xticks([1, 2, 3, 4, 5])
ax.legend()
plt.tight_layout()
plt.show()

print(length_by_star.to_string(index=False))

# #### Top reviewed and highly rated Businesses in Food & Dining


# Write the SQL query
top_rated_businesses = spark.sql("""
    SELECT 
        b.name, 
        b.city,
        b.review_count,
        b.stars as avg_rating
    FROM businesses b 
    WHERE main_category == 'Food & Dining'
    ORDER BY 3 desc,4 DESC
    LIMIT 10
""")

top_rated_businesses.show()


from pyspark.sql.functions import col, desc
top_rated_businesses_FoodDining = business_clean_df\
    .filter(business_clean_df.main_category == 'Food & Dining') \
    .select('business_id','name', 'city', 'review_count', col('stars').alias("avg_rating")) \
    .orderBy(desc("review_count"), desc("stars")) \
    .limit(10)

top_rated_businesses_FoodDining.show()


# #### Top reviewed and highly rated Businesses in Home & Local Services


from pyspark.sql.functions import col, desc
top_rated_businesses_HomeLocalServices = business_clean_df\
    .filter(business_clean_df.main_category == 'Home & Local Services') \
    .select('business_id','name', 'categories','city', 'review_count', col('stars').alias("avg_rating")) \
    .orderBy(desc("review_count"), desc("stars")) \
    .limit(10)

top_rated_businesses_HomeLocalServices.show()


import pandas as pd

# This tells Pandas to show the full content of every cell
pd.set_option('display.max_colwidth', None)

# Your query
ac = business_clean_df.filter(col("name").like("%Tampa Bay Brew%"))

# Now this will show everything
ac.toPandas()


# #### Category-Level Sentiment Analysis


# ════════════════════════════════════════════════════════════════════════
# COMBINED ANALYSIS: Category-Level Sentiment Breakdown
# Uses review stars + business categories to answer:
# "Which business categories attract the most positive vs negative reviews?"
# ════════════════════════════════════════════════════════════════════════

# Step 1: Create sentiment label using the existing stars column
review_with_sentiment = review_df.withColumn("sentiment_label",
    F.when(F.col("stars") >= 4, "Positive")
     .when(F.col("stars") <= 2, "Negative")
     .otherwise("Neutral")
)

# Step 2: Join directly using your already-defined 'main_category'
# This removes the redundant 'explode' and 'split' operations
cat_sentiment = review_with_sentiment.join(
    business_clean_df.select("business_id", "main_category"),
    "business_id", "inner"
).groupBy("main_category", "sentiment_label").count()

# Step 3: Pivot to wide format (Positive/Neutral/Negative as columns)
cat_pivot = cat_sentiment.groupBy("main_category").pivot("sentiment_label",
    ["Negative", "Neutral", "Positive"]).sum("count").fillna(0)

# Step 4: Calculate Percentages
cat_pivot = cat_pivot.withColumn(
    "total", F.col("Negative") + F.col("Neutral") + F.col("Positive")
).withColumn(
    "positive_pct", F.round(F.col("Positive") / F.col("total") * 100, 1)
).withColumn(
    "negative_pct", F.round(F.col("Negative") / F.col("total") * 100, 1)
).withColumn(
    "neutral_pct", F.round(F.col("Neutral") / F.col("total") * 100, 1)
)

# Filter for significance (e.g., categories with enough reviews to matter)
cat_pd = cat_pivot.filter(F.col("total") >= 500).orderBy("positive_pct", ascending=False).toPandas()

# ── Plot: Top & Bottom categories by positive sentiment % ─────────────────
top15    = cat_pd.head(15)
bottom15 = cat_pd.tail(15).sort_values("positive_pct")

fig, axes = plt.subplots(1, 2, figsize=(18, 7))
for ax, data, title, color in zip(
    axes, [top15, bottom15],
    ["Top Categories by Positive Review %", "Bottom Categories by Positive Review %"],
    ["#2ecc71", "#e74c3c"]
):
    # Using 'main_category' now instead of 'category'
    ax.barh(data["main_category"], data["positive_pct"], color=color, edgecolor="black")
    for i, (_, row) in enumerate(data.iterrows()):
        ax.text(row["positive_pct"] + 0.3, i, f"{row['positive_pct']}%", va="center")
    ax.set_xlim(0, 105)
    ax.set_title(title)

plt.tight_layout()
plt.show()

# ── Plot: Stacked bar (Simplified to use the new neutral_pct column) ──────
top20 = cat_pd.head(20).sort_values("positive_pct")
fig, ax = plt.subplots(figsize=(12, 8))
ax.barh(top20["main_category"], top20["positive_pct"], label="Positive", color="#2ecc71")
ax.barh(top20["main_category"], top20["neutral_pct"], left=top20["positive_pct"], label="Neutral", color="#f39c12")
ax.barh(top20["main_category"], top20["negative_pct"], label="Negative", color="#e74c3c", 
        left=top20["positive_pct"] + top20["neutral_pct"])
ax.set_title("Sentiment Breakdown by Main Category")
ax.set_xlabel("Percentage of Reviews")
ax.legend(loc="lower right")
plt.show()

# ── Table: Most negative and positive categories ──────────────────────────────────────
# Print the "Wall of Shame" (Most negative categories)
print("\nMost negatively reviewed categories (min 500 reviews):")
print(cat_pd.sort_values("negative_pct", ascending=False)[
    ["main_category", "total", "positive_pct", "negative_pct"]
].head(10).to_string(index=False))


# The "Hall of Fame" (Most positive categories)
print("\nMost positively reviewed categories (min 500 reviews):")
print(cat_pd.sort_values("positive_pct", ascending=False)[
    ["main_category", "total", "positive_pct", "negative_pct"]
].head(10).to_string(index=False))

# #### Word frequency analysis — top words in positive vs negative reviews


from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# ==========================================
# 1. TEXT PROCESSING 
# ==========================================
noise_words = ["get", "got", "told", "called", "said", "call", "came", "even", 
               "back", "one", "really", "also", "still", "it.", "take"]

# Create sentiment labels
review_labeled = review_df.withColumn("sentiment",
    F.when(F.col("stars") >= 4, "positive")
     .when(F.col("stars") <= 2, "negative")
     .otherwise(None)
).filter(F.col("sentiment").isNotNull())

# Tokenize and Clean
tokenizer = Tokenizer(inputCol="text", outputCol="raw_words")
remover = StopWordsRemover(inputCol="raw_words", outputCol="filtered_words")

df_tokenized = tokenizer.transform(review_labeled)
df_filtered = remover.transform(df_tokenized)

# Explode and Clean individual words (Master List)
words_master = df_filtered.select("business_id", "sentiment", F.explode(F.col("filtered_words")).alias("word")) \
    .filter(F.length(F.col("word")) > 2) \
    .filter(~F.col("word").isin(noise_words)) \
    .filter(~F.col("word").rlike("[^a-zA-Z]")) \
    .withColumn("word", F.lower(F.trim(F.col("word")))) \
    .withColumn("word", 
        F.when(F.col("word") == "ordered", "order")
         .when(F.col("word") == "came", "come")
         .otherwise(F.col("word"))
    ).cache() 

# ==========================================
# GRAPH 1: Global Sentiment (The "Big Picture")
# ==========================================
top_pos = words_master.filter(F.col("sentiment") == "positive").groupBy("word").count().orderBy("count", ascending=False).limit(20).toPandas()
top_neg = words_master.filter(F.col("sentiment") == "negative").groupBy("word").count().orderBy("count", ascending=False).limit(20).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 8))
for ax, data, title, color in zip(axes, [top_pos, top_neg],  
                                 ["Top Words — Positive Reviews (4-5★)", "Top Words — Negative Reviews (1-2★)"],
                                 ["#2ecc71", "#e74c3c"]):
    ax.barh(data["word"][::-1], data["count"][::-1], color=color, edgecolor="black")
    ax.set_xlabel("Frequency")
    ax.set_ylabel("Word")
    ax.set_title(title, fontweight="bold")
plt.suptitle("Most Frequent Words by Sentiment Class", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.show()

# #### Words that come the most in most reviewed restaurants


'''
# 1. Transform the 'categories' string into an array, then into individual rows
# 2. Trim whitespace to ensure " Pizza" and "Pizza" are treated as the same word

top_businesses = review_df.join(top_rated_businesses,"business_id")


all_review_words = top_businesses.withColumn("review_words", F.explode(F.split(F.lower(F.col("text")), " "))) \
    .withColumn("review_words", F.trim(F.col("review_words")))

stop_words = ["the", "and", "to", "a", "was", "of", "they", "for", "my", "that", "in", "is", "it", "with", "we", "on", "this", "not", "i", ""]

# 3. Group by the new individual words and count occurrences
word_counts_review = all_review_words.filter(~F.col("review_words").isin(stop_words)) \
    .groupBy("review_words") \
    .count() \
    .orderBy("count", ascending=False)
word_counts_review.show() 
'''

# --- Helper Function to keep styling consistent and remove redundancy ---
def plot_word_frequency(pdf, title, color):
    plt.figure(figsize=(12, 8))
    pdf = pdf.sort_values("count", ascending=True)
    bars = plt.barh(pdf['word'], pdf['count'], color=color)
    plt.title(title, fontsize=14, pad=20, fontweight='bold')
    plt.xlabel('Frequency', fontsize=12)
    plt.xlim(0, pdf['count'].max() * 1.15)
    
    for i, v in enumerate(pdf['count']):
        plt.text(v + (pdf['count'].max() * 0.01), i, f'{v:,}', 
                 color='black', va='center', fontweight='bold')
    plt.tight_layout()
    plt.show()

# ==========================================
# GRAPH 2: Top Rated Food & Dining Words
# ==========================================
top_biz_ids = business_clean_df.filter(F.col("main_category") == 'Food & Dining') \
    .orderBy(F.desc("review_count"), F.desc("stars")).limit(10).select("business_id")

top_rest_words = words_master.join(top_biz_ids, "business_id") \
    .groupBy("word").count().orderBy("count", ascending=False).limit(20).toPandas()

plot_word_frequency(top_rest_words, "Top 20 Words in Most Reviewed Restaurants", "green")

# **What the data tells:**
# Here is the breakdown of what these specific words reveal:
# 1. The "Vibe" is Positive
# With "good", "great", "best", and "definitely" ranking so high, these businesses are clearly crowd favorites. The word "worth" (8,458) is a huge indicator of quality—it usually appears in reviews saying the food was "worth the wait" or "worth the price."
# 2. It’s Likely Seafood & Comfort Food
# The specific food items tell you exactly what is on the menu:
# Oysters (15,823): This is a very high count! It’s likely the "star" dish of one or more of these top businesses.
# Shrimp & Fried: This reinforces the "Seafood Shack" or "Southern Kitchen" theme.
# Chicken & Hot: Could suggest "Nashville Hot Chicken" or simply hot, fresh fried chicken.
# 3. High Demand & Operations
# Line (10,368) & Wait (10,298): These businesses are busy. People are talking about the long lines and the time spent waiting to get in.
# 4. Order & Service: These are standard, but the fact that they are below the specific food items (like oysters) means people are more impressed by the product than the process.
# 
# 


# #### Words that come the most in least reviewed restaurants


# ==========================================
# GRAPH 3: Least Reviewed Food & Dining Words
# ==========================================
low_biz_ids = business_clean_df.filter(F.col("main_category") == 'Food & Dining') \
    .orderBy(F.asc("review_count"), F.asc("stars")).limit(10).select("business_id")

low_rest_words = words_master.join(low_biz_ids, "business_id") \
    .groupBy("word").count().orderBy("count", ascending=False).limit(20).toPandas()

plot_word_frequency(low_rest_words, "Top 20 Words in Least Reviewed Restaurants", "#c0392b")

# **What the data is telling:**
# 1. The "Frustration" Factor
# - "Never" (13) & "Asked" (10): These are red flags. In low-reviewed spots, "never" usually appears in phrases like "never got my water" or "never coming back." "Asked" usually implies a struggle to get service, like "asked for the check three times."
# - "Order" (32) is #1: Notice how "order" is much higher than "food." This suggests the process of ordering is a bigger talking point than the meal itself—likely because the orders are wrong or taking too long. 
# 2. Specific Items: You see "chicken" and "cake". In low-reviewed businesses, reviews are often very specific complaints about a single dish rather than the general "vibe" or "company."
# 3. Person-Focused: Words like "lady" and "guy" appearing suggest that customers are calling out specific employees, which is a common trend in negative or "last resort" reviews for struggling businesses.


# #### Businesses who are opened and closed


#from pyspark.sql import functions as F
#from pyspark.sql.window import Window

# 1. Map 1/0 to Open/Closed
# 2. Group and count
# 3. Calculate percentage using a Window function
businesses_status = business_clean_df.withColumn(
    "business_status", 
    F.when(F.col("is_open") == 1, "Open").otherwise("Closed")
).groupBy("business_status").count()

# Add the percentage column
total_count = business_clean_df.count()
businesses_status = businesses_status.withColumn(
    "percentage", 
    F.round((F.col("count") / total_count) * 100, 2)
)

businesses_status.show()


import matplotlib.pyplot as plt

# 1. Convert to Pandas for plotting
pandas_status = businesses_status.toPandas()

# 2. Setup the plot
plt.figure(figsize=(6, 6))

# 3. Create the Pie Chart
# We use .sum() on the pandas column to avoid the PySpark conflict
plt.pie(
    pandas_status['count'], 
    labels=pandas_status['business_status'], 
    autopct=lambda p: '{:.0f}\n({:.1f}%)'.format(p * pandas_status['count'].sum() / 100, p),
    startangle=140, 
    colors=['#8105ad', '#db2114'],
    textprops={'fontsize': 12, 'fontweight': 'bold'}
)

# 4. Final touches
plt.title('Business Status Distribution', fontsize=14)
plt.show()


# #### Trend of Reviews over years


# 1. Extract year and aggregate
trend_df = review_df.withColumn("year", F.year("date")) \
    .groupBy("year") \
    .agg(F.count("review_id").alias("review_count")) \
    .orderBy("year")

# 2. Show the result
trend_df.show()


import matplotlib.pyplot as plt
# Convert to Pandas for plotting
pandas_trend = trend_df.toPandas()
# Helper function to format numbers
def human_format(num):
    if abs(num) >= 1_000_000:
        return f'{num / 1_000_000:.1f}M'
    elif abs(num) >= 1_000:
        return f'{num / 1_000:.1f}K'
    return str(num)

plt.figure(figsize=(10, 6))

# Plotting the data
plt.plot(pandas_trend['year'], pandas_trend['review_count'], 
         marker='o', linestyle='-', color='#2c3e50', linewidth=2)

# Fix X-axis year ticks and tilt
plt.xticks(pandas_trend['year'], rotation=45)

# Add margin for labels (20% extra space at top)
plt.ylim(0, pandas_trend['review_count'].max() * 1.2)

# Apply human_format to data labels
for i in range(len(pandas_trend)):
    count = pandas_trend['review_count'].iloc[i]
    plt.annotate(
        human_format(count), # Formats 1200 -> 1.2K
        (pandas_trend['year'].iloc[i], count),
        textcoords="offset points", 
        xytext=(5, 12),      
        ha='center', 
        rotation=45,         
        fontsize=10, 
        fontweight='bold',
        color='#e67e22'
    )

# Also format the Y-axis ticks for consistency
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([human_format(x) for x in current_values])

plt.title('Review Trend Over Years', fontsize=14, pad=20)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Number of Reviews', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.3)

plt.tight_layout()
plt.show()

# #### Reviews by Month of the year and hour of Day


from pyspark.sql import functions as F

# Hour distribution
hour_df = review_df.withColumn("hour", F.hour("date")) \
    .groupBy("hour").count().orderBy("hour").toPandas()

hour_df.head(24)




# Month distribution
month_df = review_df.withColumn("month", F.month("date")) \
    .groupBy("month").count().orderBy("month").toPandas()

month_df.head(12)

import matplotlib.pyplot as plt

def human_format(num):
    if abs(num) >= 1_000_000: return f'{num / 1_000_000:.1f}M'
    elif abs(num) >= 1_000: return f'{num / 1_000:.1f}K'
    return str(int(num))

# --- GRAPH 1: HOURLY TREND ---
plt.figure(figsize=(12, 6))
plt.bar(hour_df['hour'], hour_df['count'], color='skyblue', edgecolor='#2c3e50')

plt.title('Reviews by Hour of Day', fontsize=14)
plt.xlabel('Hour (24h Format)')
plt.xticks(range(0, 24)) # Ensure every hour (0-23) is shown

# Labels tilted at 45 degrees
for i in range(len(hour_df)):
    plt.text(hour_df['hour'][i], hour_df['count'][i], 
             human_format(hour_df['count'][i]), 
             ha='center', va='bottom', rotation=45, fontweight='bold')

plt.tight_layout()
plt.show()

# --- GRAPH 2: MONTHLY TREND ---
plt.figure(figsize=(12, 6))
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
plt.bar(month_df['month'], month_df['count'], color='salmon', edgecolor='#c0392b')

plt.title('Reviews by Month', fontsize=14)
plt.xlabel('Month')
# Replace 1-12 with Jan-Dec labels
plt.xticks(ticks=range(1, 13), labels=month_names, rotation=45)

for i in range(len(month_df)):
    plt.text(month_df['month'].iloc[i], month_df['count'].iloc[i], 
             human_format(month_df['count'].iloc[i]), 
             ha='center', va='bottom', rotation=0, fontweight='bold')

plt.tight_layout()
plt.show()


# #### Users who have rated the most


from pyspark.sql import functions as F

# 1. Sort by review_count descending and take the top 10
top_users_df = user_clean_df.select("name", "review_count","yelping_since") \
    .orderBy(F.col("review_count").desc()) \
    .limit(10)



top_users_df.show()

# #### Average rating by category


avg_rating_df = business_clean_df.groupBy("main_category") \
    .agg(F.avg("stars").alias("avg_stars")) \
    .orderBy("avg_stars", ascending=False)
avg_rating_df.show(5)

# 2. Convert to Pandas for plotting
pdf = avg_rating_df.toPandas()

# Calculate the mean from your data for the reference line
overall_mean = pdf['avg_stars'].mean()

# 3. Create the graph
plt.figure(figsize=(12, 8))
bars = plt.barh(pdf['main_category'], pdf['avg_stars'], color='#3498db')

# Add labels and formatting
plt.axvline(x=overall_mean, color="gray", linestyle="--", label=f"Avg Rating ({overall_mean:.2f}★)")
plt.xlabel('Average Rating (Stars)', fontsize=12)
plt.ylabel('Category', fontsize=12)
plt.title('Average Rating by Business Category', fontsize=14)
plt.gca().invert_yaxis()  # Put the highest rated category at the top
plt.grid(axis='x', linestyle='--', alpha=0.7)

# Add data labels on the bars
for bar in bars:
    width = bar.get_width()
    plt.text(width + 0.05, bar.get_y() + bar.get_height()/2, 
             f'{width:.2f}★', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.show()