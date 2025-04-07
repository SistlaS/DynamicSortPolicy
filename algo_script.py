import pandas as pd

stats_input_file = f"distr_stats_global.csv"
df  = pd.read_csv(stats_input_file)

#TO-DO
#this should be calculated based on the params provided
num_partitions = 6

total_records = df["global_count"].sum()
ideal_rec_per_partition = total_records//num_partitions
print(f"*********IDEAL REC PER PARTITION : {ideal_rec_per_partition}*********")
#[(partition_id, start_key, end_key)]
partitions = []
curr_sum = 0
partition_id = 1

curr_part_keys = []
print(df)
for _, row in df.iterrows():
    key = int(row["key"])
    count = int(row["global_count"])

    #skewed
    #maintain only the skewed data in the partition, i.e start_key = end_key
    if count >= ideal_rec_per_partition:
        partitions.append([partition_id, key, key])
        curr_sum = 0
        partition_id +=1 
        continue
    
    curr_part_keys.append(key)
    curr_sum += count

    if curr_sum >= ideal_rec_per_partition:
        start_key = curr_part_keys[0]
        end_key = curr_part_keys[-1]
        partitions.append([partition_id, start_key, end_key])

        curr_sum = 0
        curr_part_keys = []
        partition_id +=1

if curr_part_keys:
    start_key = curr_part_keys[0]
    end_key = curr_part_keys[-1]
    partitions.append([partition_id, start_key, end_key])
print("Partition_id, start_key, end_key")
print(partitions)