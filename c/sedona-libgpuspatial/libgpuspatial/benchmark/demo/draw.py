import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def parse_json_logs(dir):
    json_records = []
    for filename in os.listdir(dir):
        path = os.path.join(dir, filename)
        with open(path) as f:
            json_records.append(json.load(f))
    df = pd.json_normalize(json_records)
    return df


# Define the formatting function
def millions_formatter(x, pos):
    if x >= 1024 * 1024:
        return f'{x / 1024 / 1024:.0f}M'
    elif x >= 1024:
        return f'{x / 1024:.0f}K'
    else:
        return f'{x:.0f}'


def draw_default(log_root):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(8, 5))

    polys = ('overture-buildings', 'postal-codes-sorted')
    labels_polys = ('Overture Buildings', 'Postal Codes')
    nodes = ('nodes', 'osm-nodes-large-sorted-corrected')
    labels_points = ('Cali Nodes', 'OSM Nodes')

    df_cpu = parse_json_logs(os.path.join(log_root, "m7i.4xlarge/default"))
    df_a10 = parse_json_logs(os.path.join(log_root, "g5.xlarge/default"))
    df_l4s = parse_json_logs(os.path.join(log_root, "g6.xlarge/default"))
    df_l40s = parse_json_logs(os.path.join(log_root, "g6e.xlarge/default"))

    for i in range(2):
        poly_name = polys[i]
        poly_label = labels_polys[i]
        for j in range(2):
            ax = axes[i][j]
            point_name = nodes[j]
            point_label = labels_points[j]

            def filter_df(df, baseline):
                df = df[
                    df['config.build_file'].str.endswith(poly_name) & df['config.stream_file'].str.endswith(point_name)
                    ]
                return df[df['config.execution'] == baseline]

            df_boost = filter_df(df_cpu, 'Boost')
            df_geos = filter_df(df_cpu, 'GEOS')
            df_rt_a10 = filter_df(df_a10, 'RT')
            df_rt_l4s = filter_df(df_l4s, 'RT')
            df_rt_l40s = filter_df(df_l40s, 'RT')
            dfs = {'Boost': df_boost['build.time'].iloc[0] + df_boost['total_query_time'].iloc[0],
                   'GEOS': df_geos['build.time'].iloc[0] + df_geos['total_query_time'].iloc[0],
                   'A10': df_rt_a10['build.time'].iloc[0] + df_rt_a10['total_query_time'].iloc[0],
                   'L4s': df_rt_l4s['build.time'].iloc[0] + df_rt_l4s['total_query_time'].iloc[0],
                   'L40s': df_rt_l40s['build.time'].iloc[0] + df_rt_l40s['total_query_time'].iloc[0]
                   }
            df = pd.DataFrame(list(dfs.items()), columns=['Category', 'Time'])
            print(log_root, poly_label, '-', point_label, df)
            # Define colors for CPU and GPU baselines
            colors = ['skyblue', 'lightcoral', 'mediumseagreen', 'forestgreen', 'darkgreen']
            ax.bar(df['Category'], df['Time'] / 1000, color=colors)
            ax.axvline(x=1.5, color='gray', linestyle='--', linewidth=2, label='CPU/GPU Baseline Separator')
            ax.set_xlabel('Category')
            ax.set_ylabel('Time (s)')
            ax.set_title(poly_label + ' - ' + point_label)

    fig.tight_layout(pad=0.1)
    fig.savefig("time.png", format='png', bbox_inches='tight')
    plt.show()


def draw_default_cost(log_root):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(8, 5))

    polys = ('overture-buildings', 'postal-codes-sorted')
    labels_polys = ('Overture Buildings', 'Postal Codes')
    nodes = ('nodes', 'osm-nodes-large-sorted-corrected')
    labels_points = ('Cali Nodes', 'OSM Nodes')

    df_cpu = parse_json_logs(os.path.join(log_root, "m7i.4xlarge/default"))
    df_a10 = parse_json_logs(os.path.join(log_root, "g5.xlarge/default"))
    df_l4s = parse_json_logs(os.path.join(log_root, "g6.xlarge/default"))
    df_l40s = parse_json_logs(os.path.join(log_root, "g6e.xlarge/default"))

    costs = [0.8064, 1.006, 0.8048, 1.861]

    for i in range(2):
        poly_name = polys[i]
        poly_label = labels_polys[i]
        for j in range(2):
            ax = axes[i][j]
            point_name = nodes[j]
            point_label = labels_points[j]

            def filter_df(df, baseline):
                df = df[
                    df['config.build_file'].str.endswith(poly_name) & df['config.stream_file'].str.endswith(point_name)
                    ]
                return df[df['config.execution'] == baseline]

            df_boost = filter_df(df_cpu, 'Boost')
            df_geos = filter_df(df_cpu, 'GEOS')
            df_rt_a10 = filter_df(df_a10, 'RT')
            df_rt_l4s = filter_df(df_l4s, 'RT')
            df_rt_l40s = filter_df(df_l40s, 'RT')
            dfs = {
                'Boost': (df_boost['build.time'].iloc[0] + df_boost['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    0],
                'GEOS': (df_geos['build.time'].iloc[0] + df_geos['total_query_time'].iloc[0]) / 1000 / 3600 * costs[0],
                'A10': (df_rt_a10['build.time'].iloc[0] + df_rt_a10['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    1],
                'L4s': (df_rt_l4s['build.time'].iloc[0] + df_rt_l4s['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    2],
                'L40s': (df_rt_l40s['build.time'].iloc[0] + df_rt_l40s['total_query_time'].iloc[0]) / 1000 / 3600 *
                        costs[
                            3]
            }
            df = pd.DataFrame(list(dfs.items()), columns=['Category', 'Cost'])
            print("Cost", poly_label, "-", point_label, dfs["GEOS"] / dfs["A10"])
            # Define colors for CPU and GPU baselines
            colors = ['skyblue', 'lightcoral', 'mediumseagreen', 'forestgreen', 'darkgreen']
            ax.bar(df['Category'], df['Cost'], color=colors)
            ax.axvline(x=1.5, color='gray', linestyle='--', linewidth=2, label='CPU/GPU Baseline Separator')
            ax.set_xlabel('Category')
            ax.set_ylabel('Costs (USD)')
            ax.set_title(poly_label + ' - ' + point_label)

    fig.tight_layout(pad=0.1)
    fig.savefig("cost.png", format='png', bbox_inches='tight')
    plt.show()


def draw_default_time_improved(log_root, log_root_improved):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(8, 5))

    polys = ('overture-buildings', 'postal-codes-sorted')
    labels_polys = ('Overture Buildings', 'Postal Codes')
    nodes = ('nodes', 'osm-nodes-large-sorted-corrected')
    labels_points = ('Cali Nodes', 'OSM Nodes')

    df_cpu = parse_json_logs(os.path.join(log_root, "m7i.4xlarge/default"))
    df_a10 = parse_json_logs(os.path.join(log_root_improved, "g5.2xlarge/default"))
    df_l4s = parse_json_logs(os.path.join(log_root_improved, "g6.2xlarge/default"))
    df_l40s = parse_json_logs(os.path.join(log_root_improved, "g6e.2xlarge/default"))

    for i in range(2):
        poly_name = polys[i]
        poly_label = labels_polys[i]
        for j in range(2):
            ax = axes[i][j]
            point_name = nodes[j]
            point_label = labels_points[j]

            def filter_df(df, baseline):
                df = df[
                    df['config.build_file'].str.endswith(poly_name) & df['config.stream_file'].str.endswith(point_name)
                    ]
                return df[df['config.execution'] == baseline]

            df_boost = filter_df(df_cpu, 'Boost')
            df_geos = filter_df(df_cpu, 'GEOS')
            df_rt_a10 = filter_df(df_a10, 'RT')
            df_rt_l4s = filter_df(df_l4s, 'RT')
            df_rt_l40s = filter_df(df_l40s, 'RT')
            dfs = {'Boost': df_boost['build.time'].iloc[0] + df_boost['total_query_time'].iloc[0],
                   'GEOS': df_geos['build.time'].iloc[0] + df_geos['total_query_time'].iloc[0],
                   'A10': df_rt_a10['build.time'].iloc[0] + df_rt_a10['total_query_time'].iloc[0],
                   'L4s': df_rt_l4s['build.time'].iloc[0] + df_rt_l4s['total_query_time'].iloc[0],
                   'L40s': df_rt_l40s['build.time'].iloc[0] + df_rt_l40s['total_query_time'].iloc[0]
                   }

            df = pd.DataFrame(list(dfs.items()), columns=['Category', 'Time'])
            print(log_root, poly_label, '-', point_label, df)
            print(log_root, poly_label, '-', point_label, "Speedup GEOS/RT L40s", dfs["GEOS"] / dfs["A10"])
            # Define colors for CPU and GPU baselines
            colors = ['skyblue', 'lightcoral', 'mediumseagreen', 'forestgreen', 'darkgreen']
            ax.bar(df['Category'], df['Time'] / 1000, color=colors)
            ax.axvline(x=1.5, color='gray', linestyle='--', linewidth=2, label='CPU/GPU Baseline Separator')
            ax.set_xlabel('Category')
            ax.set_ylabel('Time (s)')
            ax.set_title(poly_label + ' - ' + point_label)

    fig.tight_layout(pad=0.1)
    fig.savefig("time_improved.png", format='png', bbox_inches='tight')
    plt.show()


def draw_default_cost_improved(log_root, log_root_improved):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(8, 5))

    polys = ('overture-buildings', 'postal-codes-sorted')
    labels_polys = ('Overture Buildings', 'Postal Codes')
    nodes = ('nodes', 'osm-nodes-large-sorted-corrected')
    labels_points = ('Cali Nodes', 'OSM Nodes')

    df_cpu = parse_json_logs(os.path.join(log_root, "m7i.4xlarge/default"))
    df_a10 = parse_json_logs(os.path.join(log_root_improved, "g5.2xlarge/default"))
    df_l4s = parse_json_logs(os.path.join(log_root_improved, "g6.2xlarge/default"))
    df_l40s = parse_json_logs(os.path.join(log_root_improved, "g6e.2xlarge/default"))

    costs = [0.8064, 1.212, 0.9776, 2.24208]

    for i in range(2):
        poly_name = polys[i]
        poly_label = labels_polys[i]
        for j in range(2):
            ax = axes[i][j]
            point_name = nodes[j]
            point_label = labels_points[j]

            def filter_df(df, baseline):
                df = df[
                    df['config.build_file'].str.endswith(poly_name) & df['config.stream_file'].str.endswith(point_name)
                    ]
                return df[df['config.execution'] == baseline]

            df_boost = filter_df(df_cpu, 'Boost')
            df_geos = filter_df(df_cpu, 'GEOS')
            df_rt_a10 = filter_df(df_a10, 'RT')
            df_rt_l4s = filter_df(df_l4s, 'RT')
            df_rt_l40s = filter_df(df_l40s, 'RT')
            dfs = {
                'Boost': (df_boost['build.time'].iloc[0] + df_boost['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    0],
                'GEOS': (df_geos['build.time'].iloc[0] + df_geos['total_query_time'].iloc[0]) / 1000 / 3600 * costs[0],
                'A10': (df_rt_a10['build.time'].iloc[0] + df_rt_a10['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    1],
                'L4s': (df_rt_l4s['build.time'].iloc[0] + df_rt_l4s['total_query_time'].iloc[0]) / 1000 / 3600 * costs[
                    2],
                'L40s': (df_rt_l40s['build.time'].iloc[0] + df_rt_l40s['total_query_time'].iloc[0]) / 1000 / 3600 *
                        costs[
                            3]
            }
            df = pd.DataFrame(list(dfs.items()), columns=['Category', 'Cost'])
            print("Cost", poly_label, "-", point_label, dfs["GEOS"] / dfs["A10"])
            print(log_root, poly_label, '-', point_label, "Speedup GEOS/RT L40s", dfs["GEOS"] / dfs["L40s"])
            # Define colors for CPU and GPU baselines
            colors = ['skyblue', 'lightcoral', 'mediumseagreen', 'forestgreen', 'darkgreen']
            ax.bar(df['Category'], df['Cost'], color=colors)
            ax.axvline(x=1.5, color='gray', linestyle='--', linewidth=2, label='CPU/GPU Baseline Separator')
            ax.set_xlabel('Category')
            ax.set_ylabel('Costs (USD)')
            ax.set_title(poly_label + ' - ' + point_label)

    fig.tight_layout(pad=0.1)
    fig.savefig("cost_improved.png", format='png', bbox_inches='tight')
    plt.show()


# draw_default("./logs_showcase_midterm")
# draw_default_cost("./logs_showcase_midterm")
# draw_default_time_improved("./logs_showcase_midterm","./logs_showcase_final")
draw_default_cost_improved("./logs_showcase_midterm","./logs_showcase_final")
