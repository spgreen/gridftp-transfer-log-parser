import datetime
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys


def gridftp_datetime_conversion(*gridftp_datetime):
    """
    
    :param gridftp_datetime: 
    :return: 
    """
    return [datetime.datetime.strptime(g_datetime, "%Y%m%d%H%M%S.%f") for g_datetime in gridftp_datetime]


def abs_difference(datetime1, datetime2):
    """
    Calculates the absolute value of the datetime difference
    :param datetime1: 
    :param datetime2: 
    :return: 
    """
    return abs((datetime1-datetime2).total_seconds())


def calculate_throughput(file_size, start_datetime, end_datetime):
    """
    Calculates the throughput in MB/s from dividing the file size by time taken to download said file
        >>> calculate_throughput(1000000000, '20170817062939.888844', '20170817072939.888844')
        0.28

    :param file_size: file size in bytes
    :param start_datetime: date time in the form of YmdHMS.f e.g. 20170817062939.888844
    :type start_datetime: str
    :param end_datetime: date time in the form of YmdHMS.f e.g. 20170817062939.888844
    :type end_datetime: str
    :return: 
    """
    start_t, end_t = gridftp_datetime_conversion(start_datetime, end_datetime)
    return round(8*(file_size/abs_difference(start_t, end_t))*10**-9, 2)


def convert_bytes_to_megabytes(*values):
    conversion = 10**-6
    return [float(value)*conversion for value in values]


def retrieve_and_display_logs_from_file(fp):
    with open(fp, 'r') as log_file:
        date_time_list = []
        destination_list = []
        source_list = []
        ftp_type_list = []
        file_size_list = []
        streams_list = []
        throughput_list = []
        buffer_size_list = []
        block_size_list = []

        print('DateTime Host Type FileSize FileDestination Streams Throughput_Gbps BufferSize_MB BlockSize_MB')
        for log_info in log_file.readlines():
            log_info = log_info.strip()
            if not log_info:
                continue
            if log_info[0] == '#':
                print("\n%s" % log_info)
                continue

            if ('Transfer stats:' and 'TYPE') in log_info:
                test_end = re.search("DATE=([0-9]{14}\.[0-9]{6})", log_info).group(1)
                test_start = re.search("START=([0-9]{14}\.[0-9]{6})", log_info).group(1)
                file_size = float(re.search("NBYTES=([0-9]+) ", log_info).group(1))
                streams = int(re.search("STREAMS=([0-9]+) ", log_info).group(1))
                ftp_type = re.search("TYPE=([A-Z]{4})", log_info).group(1)
                destination = re.search("DEST=\[(.*)\] ", log_info).group(1)
                source = re.search("HOST=([^\s]+)", log_info).group(1)
                file_destination = re.search("FILE=([^\s]+)", log_info).group(1)
                if ftp_type == 'MLSD':
                    continue
                buffer = int(re.search("BUFFER=([0-9]+) ", log_info).group(1))
                block = int(re.search("BLOCK=([0-9]+)", log_info).group(1))

                test_time = gridftp_datetime_conversion(test_end)[0] #.strftime('%Y-%m-%d_%H:%M:%S')
                throughput = calculate_throughput(file_size, test_start, test_end)
                if not throughput: continue
                file_size = round(file_size*10**-9, 2)
                buffer_MB, block_MB = convert_bytes_to_megabytes(buffer, block)

                print(test_time, destination, ftp_type, file_size, file_destination,
                      streams, throughput, buffer_MB, block_MB)

                date_time_list.append(test_time)
                destination_list.append(destination)
                source_list.append(source)
                ftp_type_list.append(ftp_type)
                file_size_list.append(file_size)
                streams_list.append(streams)
                throughput_list.append(throughput)
                buffer_size_list.append(buffer_MB)
                block_size_list.append(block_MB)

    return {'date_time': date_time_list,
            'source': source_list,
            'destination': destination_list,
            'ftp_type': ftp_type_list,
            'file_size': file_size_list,
            'p_streams': streams_list,
            'throughput': throughput_list,
            'buffer_size': buffer_size_list}  # 'block_size': block_size_list}


def pd_df(df):
    connections = df['connection'].unique()
    df = [df[df['connection'] == i] for i in connections]
    # mask = (df[0]['throughput'] > 4) & (df[0]['throughput'] < 14)
    # df[0] = df[0][mask]

    for index, obj in enumerate(df):
        plt.figure(index)

        plt.subplot(331)

        plt.ylabel('File Size (GB)')
        plt.scatter(obj['file_size'], obj['throughput'])
        plt.grid(b=True, which='major', axis='x', color='grey', linestyle='--')
        plt.grid(b=True, which='major', axis='y', color='grey', linestyle='--')
        plt.minorticks_on()

        plt.subplot(332)
        plt.title(connections[index])
        plt.ylabel('No. of Streams')
        plt.scatter(obj['p_streams'],obj['throughput'])
        plt.grid(b=True, which='major', axis='x', color='grey', linestyle='--')
        plt.grid(b=True, which='major', axis='y', color='grey', linestyle='--')
        plt.minorticks_on()

        plt.subplot(333)
        plt.ylabel('Buffer Size (MB)')
        plt.scatter(obj['buffer_size'], obj['throughput'])
        plt.grid(b=True, which='major', axis='x', color='grey', linestyle='--')
        plt.grid(b=True, which='major', axis='y', color='grey', linestyle='--')
        plt.minorticks_on()
        plt.xlabel('Throughput (Gbps)')

        # plt.tight_layout(h_pad=0)

    plt.show()


def graph_variables(data_frame, graph_entities):
    for index, obj2 in enumerate(graph_entities):
        count = index + 1
        for obj3 in graph_entities:
            plt.subplot(length, length, count)
            plt.scatter(data_frame[obj2], data_frame[obj3])
            if (obj3 is 'throughput' and obj2 is 'p_streams') or (obj2 is 'throughput' and obj3 is 'p_streams'):
                m, b = np.polyfit(data_frame[obj2], data_frame[obj3], 1)
                plt.plot(data_frame[obj2], m * data_frame[obj2] + b, 'r-')

            plt.grid(b=True, which='major', axis='x', color='grey', linestyle='--')
            plt.grid(b=True, which='major', axis='y', color='grey', linestyle='--')

            # Sets y label for left most side of the figure
            if not ((count - 1) % length):
                plt.ylabel(obj3)
            if count > (length * 3):
                plt.xlabel(obj2)
            count += length


if __name__ == '__main__':
    log_path = sys.argv[1]
    log = retrieve_and_display_logs_from_file(log_path)

    df = pd.DataFrame(log)

    graph_entities = list(df.select_dtypes(include=['float64', 'int64']))
    length = len(graph_entities)
    del log
    # print(df.head(), '\n')
    # Creates list with each host belonging to an index
    destinations = df['destination'].unique()
    sources = df['source'].unique()
    file_size = df['file_size'].unique()
    dfs = [df[df['destination'] == i] for i in destinations]

    for figure_number, data_frame in enumerate(dfs):
        plt.figure(figure_number)
        plt.suptitle(*data_frame['destination'].unique())
        print(*data_frame['destination'].unique())
        graph_variables(data_frame, graph_entities)
    plt.show()
