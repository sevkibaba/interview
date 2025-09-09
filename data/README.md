# Python Coding Exercise

Your task is to build a python script to gather data from NASA's Near Earth Object Web Service API, and save that data. We'll also perform some aggregations to make reporting on Near Earth Objects simpler for our theoretical website.

The page for the API is here: https://api.nasa.gov

To save our data, we'll write it out to the local filesystem as if we're saving it to an S3 Data Lake. This will save having to mess with AWS credentials. Your files should be saved in the same data directory in which this README resides, in whatever folder structure you would use to save the data in S3.

### Requirements
- Create an account at [api.nasa.gov](https://api.nasa.gov) to get an API key
- Find the docs for the Near Earth Object Web Service (below the signup on the same page)
- Data should be saved in Parquet format
- Design the code such that the scraping and processing part could easily be scaled up to tens of GBs of data but it can also be easily run 
locally at development scale. 
- Use the Browse API to request data
    - There are over 1800 pages of near Earth objects, so we'll limit ourselves to gathering the first 200 near earth objects
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

### Submitting your coding exercise
Once you have finished your script, please create a PR into Tekmetric/interview. Don't forget to update the gitignore if that is required!

### Additional Notes
- The raw data should be recorded in Avro for a longer term operation since it is easy change schema and has high throughput than parquet files.
- In addition to record raw data and having api requests in another module is a better approach than having them all in a spark job. The machines used for spark jobs are expensive for an api-scrape operation, and the parallel executor architecture is not suitable for api scraping because it lacks the politeness in its nature. Moreover, it would be very expensive and complex to handles errors on the fly.
- Recording raw data in its own place is better for future cases. The new report requirements could be build on top of that.
- It would be better to have a rate limiter that also takes the api limits into account. Since there is a limit in the api requests in an hourly manner, tha api scraper part is not designed to scale more.
- There should be an alerting system and another folder to record unexpected data after a validation step. It could be the case that the incoming data is mission critical and needs to be handled properly.
- The dataframe operation can be refactorred to pyarrow operations for better performance. 