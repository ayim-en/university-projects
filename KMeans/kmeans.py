from pyspark import SparkContext
import sys
import math

# Function to calculate the Euclidean distance between two points
def distance(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

# Function to find the index of the closest center for a given point
def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        dist = distance(p, centers[i])
        if dist < closest:
            closest = dist
            bestIndex = i
    return bestIndex

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: kmeans.py <input_directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="KMeansDeviceLocation")
    
    # Load the ETL output data
    lines = sc.textFile(sys.argv[1])
    
    # format: date, manufacturer, device ID, latitude, longitude
    def parse_line(line):
        parts = line.split(',')
        return (float(parts[3]), float(parts[4]))
        
    parsed_data = lines.map(parse_line)
    
    # Filter out known (0.0, 0.0) locations
    valid_locations = parsed_data.filter(lambda p: p[0] != 0.0 or p[1] != 0.0)
    
    # Persist this RDD because it is evaluated iteratively inside the while loop
    valid_locations.persist()
    
    # Initialize variables according to assignment requirements
    K = 5
    convergeDist = 0.1
    # data.takeSample(withReplacement, num, seed)
    centers = valid_locations.takeSample(False, K, 34)
    
    tempDist = float("inf")
    
    # Iterative K-means algorithm
    while tempDist > convergeDist:
        # Assign each point to the closest center
        closest = valid_locations.map(lambda p: (closestPoint(p, centers), (p, 1)))
        
        # Calculate the sum of points and count per cluster
        pointStats = closest.reduceByKey(
            lambda p1, p2: ((p1[0][0] + p2[0][0], p1[0][1] + p2[0][1]), p1[1] + p2[1])
        )
        
        # Compute the new centers
        newPoints = pointStats.map(
            lambda st: (st[0], (st[1][0][0] / st[1][1], st[1][0][1] / st[1][1]))
        ).collect()
        
        # Calculate the total distance the centers have moved
        tempDist = 0.0
        for (i, p) in newPoints:
            tempDist += distance(centers[i], p)
            centers[i] = p
            
    for pt in centers:
        print(f"[{pt[0]}, {pt[1]}]")
        
    sc.stop()