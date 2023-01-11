from pyspark import SparkConf, SparkContext

START_CHARACTER_ID = 5306
TARGET_CHARACTER_ID = 14


def convert_to_bfs(line: str):
    fields = line.split()
    hero_id = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]
    color = "WHITE"
    distance = 9999

    if hero_id == START_CHARACTER_ID:
        color = "GRAY"
        distance = 0

    return hero_id, (connections, distance, color)


def bfs_map(node):
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []
    if color == "GRAY":
        for new_character_id in connections:
            new_distance = distance + 1
            new_color = "GRAY"
            if new_character_id == TARGET_CHARACTER_ID:
                hit_counter.add(1)

            results.append((new_character_id, ([], new_distance, new_color)))

        color = "BLACK"

    results.append((character_id, (connections, distance, color)))
    return results


def bfs_reduce(node1, node2):
    connections1 = node1[0]
    connections2 = node2[0]

    distance1 = node1[1]
    distance2 = node2[1]

    color1 = node1[2]
    color2 = node2[2]

    new_connections = connections1 + connections2
    new_distance = min(distance1, distance2)
    new_color = "WHITE"
    if color1 == "GRAY" or color2 == "GRAY":
        new_color = "GRAY"
    if color1 == "BLACK" or color2 == "BLACK":
        new_color = "BLACK"

    return new_connections, new_distance, new_color


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("BFS")
    sc = SparkContext(conf=conf)
    hit_counter = sc.accumulator(0)
    iteration_rdd = sc.textFile("./marvel_graph.txt").map(convert_to_bfs)
    while hit_counter.value == 0:
        mapped = iteration_rdd.flatMap(bfs_map)
        print("Processing " + str(mapped.count()) + " values.")
        iteration_rdd = mapped.reduceByKey(bfs_reduce)
    print(f"Hit the target character! From {hit_counter.value} different direction(s).")
