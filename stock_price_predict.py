


from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

#------------------------------------------------------------------------------
# if you are using alpha_vantage for first time you can use {pip install alpha_vantahe or you can use any other website and their api also}
from alpha_vantage.timeseries import TimeSeries
from pyspark.sql import SparkSession

# Set your Alpha Vantage API key
api_key = "<api_key>"
# Create an instance of the TimeSeries class
ts = TimeSeries(key=api_key, output_format='pandas')

# Specify the stock symbol you want to retrieve data for
stock_symbol = "AAPL" # it defines apple stock , you can use different symbols for different stock , check alpha_vantage documentation for futher assistance

# Specify the interval for the streaming data (e.g., '1min', '5min', '15min', etc.)

# the following varriable is used when you use the " ts.get_intraday" function only 
#interval = "15min" 

# Retrieve the streaming data
#data, _ = ts.get_intraday(symbol=stock_symbol, interval=interval, outputsize='full')
data, _ = ts.get_daily_adjusted(symbol=stock_symbol, outputsize='full')


# Define the schema for the data
schema = "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE,  Volume LONG"

# Specify the path to save the data
csv_path = "/home/sysadm/Documents/spark/data.csv" # provide your desired location

# Save the data to a CSV file
data.to_csv(csv_path)

# Create a SparkSession
spark = SparkSession.builder.appName("StockPrediction").getOrCreate()

# Read the data from the CSV file
streaming_data = spark.read.format("csv").schema(schema).option("header", "true").load(csv_path)
#--------------------------------------------------------------------------------------------------
# Step 1: Create a Spark session
spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()

# Step 2: Load the CSV data into a DataFrame
data = spark.read.csv("data.csv", header=True, inferSchema=True)

# Remove the desired columns
columns_to_remove = ['7. dividend amount', '8. split coefficient','5. adjusted close']
data = data.drop(*columns_to_remove)
data=data.withColumnRenamed('1. open','open').withColumnRenamed('2. high','high').withColumnRenamed('3. low','low').withColumnRenamed('6. volume','volume').withColumnRenamed('4. close','close')

# Step 3: Preprocess the data
# Convert the date column to a suitable format
data = data.withColumn("date", data["date"].cast("timestamp"))

# Handle missing values if any
data = data.na.drop()

# Step 4: Split the data into training and testing datasets
(trainingData, testingData) = data.randomSplit([0.7, 0.3])

# Step 5: Define and train a regression model
assembler = VectorAssembler(inputCols=["open", "high", "low", "volume"], outputCol="features")
regressor = RandomForestRegressor(featuresCol="features", labelCol="close", numTrees=10)
model = regressor.fit(assembler.transform(trainingData))

# Step 6: Generate predictions on the testing dataset
predictions = model.transform(assembler.transform(testingData))

# Step 7: Prepare the data for plotting
actual_prices = predictions.select("close").rdd.flatMap(lambda x: x).collect()
predicted_prices = predictions.select("prediction").rdd.flatMap(lambda x: x).collect()



# Step 7: Calculate accuracy
actual_prices = predictions.select("close").rdd.flatMap(lambda x: x).collect()
predicted_prices = predictions.select("prediction").rdd.flatMap(lambda x: x).collect()

def calculate_mape(actual, predicted):
    errors = []
    for i in range(len(actual)):
        if actual[i] != 0:
            error = abs((actual[i] - predicted[i]) / actual[i])
            errors.append(error)
    mape = sum(errors) / len(errors)
    return mape

mape = calculate_mape(actual_prices, predicted_prices)
accuracy = 1 - mape

print("Accuracy:", accuracy)
#print(predicted_prices)

# you can do further plottings and additional things that you like 
# THANK YOU 
