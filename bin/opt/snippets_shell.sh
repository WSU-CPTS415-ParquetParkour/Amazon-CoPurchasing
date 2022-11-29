# Shell command snippets

# Get db size
sudo podman exec neo4j du -hc $NEO4J_HOME/data/databases/

# Get count of reviews
grep -n ./data/amazon-meta.txt -e "reviews:" | awk -F ' ' '{print $4}' | awk '{s+=$1} END {printf"%.0f\n", s}'

# Get count of unique category paths
grep ./data/amazon-meta.txt -e "|" | sort --unique | wc -l

# Get count of similar items
grep -n ./data/amazon-meta.txt -e "similar:" | awk -F ' ' '{print $3}' | awk '{s+=$1} END {printf"%.0f\n", s}'

# Get count of reviews
grep -n ./data/amazon-meta.txt -e "reviews:" | awk -F ' ' '{print $4}' | awk '{s+=$1} END {printf"%.0f\n", s}'

# Get count of non-unique category paths
grep -n ./data/amazon-meta.txt -e "categories:" | awk -F ' ' '{print $3}' | awk '{s+=$1} END {printf"%.0f\n", s}'

# Get lines that contain solo backslashes
grep -n ./data/amazon-meta.txt -e "[\]"


