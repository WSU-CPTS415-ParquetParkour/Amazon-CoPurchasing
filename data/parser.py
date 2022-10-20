def load(filename):
    # open dataset fiel to read
    data=dict()
    with open(filename, 'r', 1, "utf-8") as dataset:
        for line in dataset:

            #this is a comment line
            if line.startswith('#'):
                continue

            else:

                # when line starts with id, then its a new product
                if line.startswith("Id"):
                    current_id = line.strip().split()[1]
                    data[current_id] = dict();

                # line starts with ASIN
                elif line.startswith("ASIN"):
                    data[current_id]['ASIN'] = line.strip().split()[1]

                # line starts with title
                elif "title" in line:
                    data[current_id]["title"] = line[8:].strip()

                # line starts with group
                elif "group" in line:
                    data[current_id]['group'] = line.strip().split()[1]

                # line starts with salesrank
                elif "salesrank" in line:
                    data[current_id]['salesrank'] = line.strip().split()[1]

                # line starts with simliar
                elif "similar" in line:
                    data[current_id]['similar'] = line.strip().split()[2:]

                elif "discontinued product" in line:
                    data[current_id]['similar'] = "n"
                    data[current_id]["title"] = "n"
                    data[current_id]['group'] = "n"
                    data[current_id]['salesrank'] = "n"

                # line starts with reviews
                #elif "reviews" in line:
                    #data[current_id]['reviews'] = line.strip().split()[1]
    return data

# list of adjacent ASINs
def returnsimilar(data):
    adj = []
    for key in data:
        for item in data[key]['similar']:
            adj.append([int(key), item])
    return adj
    
def rawdatatocsv(rawdata, csvfile):
    with open(csvfile,'w', 1, "utf-8") as csv:
        csv.write('Id,ASIN,title,group,salerank,similar\n')
        for product in rawdata:
            csv.write(str(product[0]) + ','+  str(rawdata[product]['ASIN'])  + ','+  str(rawdata[product]['title']) + ',' +  str(rawdata[product]['group']) + ',' +  str(rawdata[product]['salesrank']) + ',' +  str(rawdata[product]['similar']) + '\n')

def similartocsv(similardata, csvfile):
    with open(csvfile,'w', 1, "utf-8") as csv:
        csv.write('Id,similarASIN\n')
        for product in similardata:
            csv.write(str(product[0]) + ',' + str(product[1]) + '\n')

def main():

    #dict of raw data
    dataset = load("sample.txt")
    # edges
    adjset = returnsimilar(dataset)
    
    #to csv functions
    #rawdatatocsv(dataset, "rawdata.csv")
    #similartocsv(adjset, "similar.csv")
    print(dataset)
    
   
if __name__ == "__main__":
    main()