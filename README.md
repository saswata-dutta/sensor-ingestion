### Maintenance Prediction

    Steps:
    - profiles = fetch profiles from s3 (decide path scheme)
    - profiledVehicles = extract list of vehicles with profiles
    - candidateVehicles = profiledVehicles intersect sampleSet (of vehicles)
    - fetch last 12 hrs continuous buckets
    - fetch last 3 hours data of each candidateVehicles
        + create 15 min buckets, compute distances travelled in that bucket
        + iterate backwards find buckets until 200km distance travelled fetch from s3 one hr bucket at a time
        + create profile
        + compare with existing profile
        + accumulate in alerting list if needed
        + accumulate current bucket in ongoing 3hr buckets
    - persist in s3 current ongoing bucket of buckets per 3hr level (partitioned on y/m/d/h)
