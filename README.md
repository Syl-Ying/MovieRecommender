# 🎬 Movie Recommender System (Item-Based Collaborative Filtering)

## 📌 Features

	•	Item-based collaborative filtering
	•	Cosine similarity for item similarity scoring
	•	Implemented in Java for Hadoop MapReduce
	•	Designed for deployment on AWS EMR
	•	Supports MovieLens 100K and 1M datasets
	•	Multi-step pipeline: rating pair generation, similarity computation, sorting

## 🚀 Architecture
  
The recommendation pipeline consists of three MapReduce steps:
  
1.	Step 1 – Rating Pair Generation
   
Generate all co-rated movie pairs with rating pairs from each user.
  
2.	Step 2 – Similarity Computation
   
Compute cosine similarity for each movie pair using the aggregated rating pairs.
  
3.	Step 3 – Sorting Similar Movies
   
Sort and filter results to output the top similar movies for each target movie.

## 🗂️ Dataset

Uses the MovieLens datasets:
	•	u.data for user-movie ratings
	•	u.item for movie metadata

The 100k datasets was used for local testing and debugging meanwhile the 1M dataset was used for AWS EMR deployment.


## 📈 Sample Output
Movie: Star Wars (1977)

Similar: Empire Strikes Back, The (1980) (Score: 0.92)

Similar: Raiders of the Lost Ark (1981) (Score: 0.89)
