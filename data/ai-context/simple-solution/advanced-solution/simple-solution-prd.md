# Simple Solution

Your task is to build a python script to gather data from NASA's Near Earth Object Web Service API, and save that data to the output directory under simple-solution. We'll also perform some aggregations to make reporting on Near Earth Objects simpler for our theoretical website.

The page for the API is here: https://api.nasa.gov


### Requirements

- Data should be saved in Parquet format

- Use the Browse API to request data
    - limit to gathering the first 200 near earth objects
- We want to save the following columns in our file(s):
    - id
    - neo_reference_id
    - name
    - name_limited
    - designation
    - nasa_jpl_url
    - absolute_magnitude_h
    - is_potentially_hazardous_asteroid
    - minimum estimated diameter in meters
    - maximum estimated diameter in meters
    - **closest** approach miss distance in kilometers
    - **closest** approach date
    - **closest** approach relative velocity in kilometers per second
    - first observation date
    - last observation date
    - observations used
    - orbital period
- Store the following aggregations:
    - The total number of times our 200 near earth objects approached closer than 0.2 astronomical units (found as miss_distance.astronomical)
    - The number of close approaches recorded in each year present in the data


### Task List
1 - Create a neo api service that can fetch data from api with batches, the batch size should be a parameter
2 - Create a writer service record the batches in memory and into parquet files under output directory under raw folder
2 - Calculate the aggregations on the fly and record the results into aggregations folder under output folder in parquet format
3 - use simple-solution/recall_data.py to use service functions
4 - create a simple rate limit while hitting the api, like a simple time.sleep() function, make at most two requests per second
