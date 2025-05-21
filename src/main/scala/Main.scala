import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, IntegerType}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Recommandation de Film")
      .master("local[*]")
      .getOrCreate()


    println("Chargement des données...")

    val movies = spark.read.option("header", "true").csv("src/ml-32m/movies.csv")
    val ratingsRaw = spark.read.option("header", "true").csv("src/ml-32m/ratings.csv")

    val ratings = ratingsRaw
      .filter(col("userId").isNotNull && col("movieId").isNotNull && col("rating").isNotNull)
      .withColumn("userId", col("userId").cast(IntegerType))
      .withColumn("movieId", col("movieId").cast(IntegerType))
      .withColumn("rating", col("rating").cast(DataTypes.FloatType))

    println("✅ Données chargées et transformées.")

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2), seed = 42)

    val targetUserId =  1

    println(s"📈 Entraînement du modèle ALS pour l'utilisateur $targetUserId...")

    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setMaxIter(10)
      .setRegParam(0.1)
      .setRank(10)
      .setColdStartStrategy("drop")

    val model = als.fit(training)

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"📊 RMSE du modèle : $rmse")

    println("🔍 Génération des recommandations...")

    val allMovieIds = ratings.select("movieId").distinct()
    val ratedMovieIds = ratings.filter(col("userId") === targetUserId).select("movieId")
    val unseenMovieIds = allMovieIds.except(ratedMovieIds)
    val userUnseenMovies = unseenMovieIds.withColumn("userId", lit(targetUserId))

    val userRecommendations = model.transform(userUnseenMovies)
      .filter(col("prediction") > 3.5)
      .select("movieId", "prediction")
      .orderBy(col("prediction").desc)
      .limit(10)

    val moviesWithIntId = movies.withColumn("movieId", col("movieId").cast(IntegerType))
    val topMovies = userRecommendations
      .join(moviesWithIntId, "movieId")
      .select("title", "prediction")

    println(s"\n🎬 Top 10 recommandations pour l'utilisateur $targetUserId :")
    topMovies.show(true)
    println(s"Nombre total de films : ${movies.count()}")
    println(s"Nombre total de notes : ${ratings.count()}")
    println(s"Nombre de films vus par l'utilisateur $targetUserId : ${ratedMovieIds.count()}")
    println(s"Nombre de films non vus : ${userUnseenMovies.count()}")



    println("✅ Processus terminé avec succès.")
    spark.stop()
  }
}
