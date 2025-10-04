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


def draw_vary_batch_size():
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(11, 5))
    df = parse_json_logs("logs/vary_batch_size")
    key = 'config.batch_size'
    val = 'total_query_time'

    def filter_df(name, parallelism):
        return df[(df["config.execution"] == name) & (df["config.parallelism"] == parallelism)].sort_values(
            key).reset_index()

    df_geos_1 = filter_df("GEOS", 1)
    df_geos_16 = filter_df("GEOS", 16)
    df_boost_1 = filter_df("Boost", 1)
    df_boost_16 = filter_df("Boost", 16)
    df_rt_1 = filter_df("RT", 1)
    df_rt_4 = filter_df("RT", 4)

    def add_fig_time(ax):
        df_geos_1.plot(ax=ax, x=key, y=val, label='GEOS_1', marker='o')
        df_geos_16.plot(ax=ax, x=key, y=val, label='GEOS_16', marker='x')
        df_boost_1.plot(ax=ax, x=key, y=val, label='Boost_1', marker='o')
        df_boost_16.plot(ax=ax, x=key, y=val, label='Boost_16', marker='x')
        df_rt_1.plot(ax=ax, x=key, y=val, label='RT_1', marker='o')
        df_rt_4.plot(ax=ax, x=key, y=val, label='RT_4', marker='x')

        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
        ax.set_xlabel('Record Batch Size')
        ax.set_ylabel('Query Time (ms)')
        ax.legend()
        ax.set_title("(a) Query time by varying record batch size")

    def add_fig_speedup(ax):
        key = 'config.batch_size'
        val = 'total_query_time'

        ax.plot(df_geos_1[key], df_geos_1[val] / df_rt_1[val], marker='o', label="Speedup over GEOS (Serial)")
        ax.plot(df_boost_1[key], df_boost_1[val] / df_rt_1[val], marker='o', label="Speedup over Boost (Serial)")
        ax.plot(df_geos_16[key], df_geos_16[val] / df_rt_4[val], marker='x',
                label="Speedup over GEOS (CPU 16 threads vs GPU 4 threads)")
        ax.plot(df_boost_16[key], df_boost_16[val] / df_rt_4[val], marker='x',
                label="Speedup over GEOS (CPU 16 threads vs GPU 4 threads)")

        ax.set_xscale('log', base=2)
        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
        ax.set_xlabel('Record Batch Size')
        ax.set_ylabel('Speedup')
        ax.set_ylim(top=25)
        ax.legend()
        ax.set_title("(b) Speed up by varying record batch size")

    add_fig_time(axes[0])
    add_fig_speedup(axes[1])

    fig.tight_layout(pad=0.1)
    fig.savefig("vary_params_batch_size.png", format='png', bbox_inches='tight')
    plt.show()


def draw_vary_parallelism():
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(9, 4))
    df = parse_json_logs("logs/vary_parallelism")
    key = 'config.parallelism'
    val = 'total_query_time'

    df_geos = df[df["config.execution"] == "GEOS"].sort_values(key).reset_index()
    df_boost = df[df["config.execution"] == "Boost"].sort_values(key).reset_index()
    df_rt = df[df["config.execution"] == "RT"].sort_values(key).reset_index()

    def add_fig_time(ax):
        df_geos.plot(ax=ax, x=key, y=val, label='GEOS', marker='o')
        df_boost.plot(ax=ax, x=key, y=val, label='Boost', marker='')
        df_rt.plot(ax=ax, x=key, y=val, label='RT', marker='x')

        # ax.set_yscale('log')
        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
        ax.set_xlabel('Number of Threads')
        ax.set_ylabel('Query Time (ms)')
        ax.legend()
        ax.set_title("(a) Query time by varying # of Threads")

    def add_fig_speedup(ax):
        ax.plot(df_rt[key], df_geos[val].head(4) / df_rt[val], marker='o', label="Speedup over GEOS")
        ax.plot(df_rt[key], df_boost[val].head(4) / df_rt[val], marker='o', label="Speedup over Boost")
        ax.set_xticks(range(1, 5))
        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
        ax.set_xlabel('Number of Threads')
        ax.set_ylabel('Speedup')
        ax.legend()
        ax.set_title("(b) Speedup by varying # of Threads")

    add_fig_time(axes[0])
    add_fig_speedup(axes[1])

    fig.tight_layout(pad=0.1)
    fig.savefig("vary_params_parallelism.png", format='png', bbox_inches='tight')
    plt.show()


def draw_breakdown():
    fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(20, 18))
    df = parse_json_logs("logs/vary_batch_size")
    key = 'config.batch_size'

    def filter_df(name, parallelism):
        return df[(df["config.execution"] == name) & (df["config.parallelism"] == parallelism)].sort_values(
            key).reset_index()

    df_geos_1 = filter_df("GEOS", 1)
    df_geos_16 = filter_df("GEOS", 16)
    df_boost_1 = filter_df("Boost", 1)
    df_boost_16 = filter_df("Boost", 16)
    df_rt_1 = filter_df("RT", 1)
    df_rt_4 = filter_df("RT", 4)

    def add_fig_time(ax, batch_size):
        first_row_geos_1 = df_geos_1[df_geos_1['config.batch_size'] == batch_size].iloc[[0]]
        first_row_geos_16 = df_geos_16[df_geos_16['config.batch_size'] == batch_size].iloc[[0]]
        first_row_boost_1 = df_boost_1[df_boost_1['config.batch_size'] == batch_size].iloc[[0]]
        first_row_boost_16 = df_boost_16[df_boost_16['config.batch_size'] == batch_size].iloc[[0]]
        first_row_rt_1 = df_rt_1[df_rt_1['config.batch_size'] == batch_size].iloc[[0]]
        first_row_rt_4 = df_rt_4[df_rt_4['config.batch_size'] == batch_size].iloc[[0]]

        df_draw = pd.concat([first_row_geos_1, first_row_geos_16, first_row_boost_1, first_row_boost_16, first_row_rt_1,
                             first_row_rt_4])
        df_draw.set_index(df_draw['config.execution'].astype(str) + '_' + df_draw['config.parallelism'].astype(str),
                          inplace=True)
        df_draw['push_build_time'] = df_draw['push_build'].apply(
            lambda arr: sum(item['time'] for item in arr) if arr else 0)

        df_draw = df_draw[
            ["total_query_time", "download.build_file_time", "download.stream_file_time", "push_build_time",
             "build.time"]]
        df_draw.columns = ("Query Time", "Download Polygons", "Download Points", "Read Polygons", "Build Index")
        df_draw.plot.bar(stacked=True, ax=ax)
        ax.tick_params(axis='x', rotation=0)
        ax.set_ylabel('Time (ms)')
        batch_size = millions_formatter(batch_size, None)
        ax.set_title(f"Execution Time Breakdown, batch size = {batch_size}")

    batch_size_list = [[128, 512, 2048],
                       [4096, 16384, 65536],
                       [131072, 262144, 524288]]
    for i in range(3):
        for j in range(3):
            add_fig_time(axes[i][j], batch_size_list[i][j])

    fig.tight_layout(pad=0.1)
    fig.savefig("time_breakdown.png", format='png', bbox_inches='tight')
    plt.show()


def draw_filter_time():
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6, 5))
    df = parse_json_logs("logs/vary_batch_size")
    key = 'config.batch_size'
    val = 'total_query_time'

    df_rt_1 = df[(df["config.execution"] == "RT") & (df["config.parallelism"] == 1)].sort_values(key).reset_index()
    df_rt_filter_1 = df[(df["config.execution"] == "RTFilter") & (df["config.parallelism"] == 1)].sort_values(
        key).reset_index()

    def add_fig_time(ax):
        df_rt_1.plot(ax=ax, x=key, y=val, label='RT', marker='o')
        df_rt_filter_1.plot(ax=ax, x=key, y=val, label='RT Filter Only', marker='x')

        # ax.set_xscale('log', base=2)
        # ax.set_yscale('log')
        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
        ax.set_xlabel('Record Batch Size')
        ax.set_ylabel('Query Time (ms)')
        ax.legend()
        ax.set_title("Query Time vs Filter Time")

    add_fig_time(axes)
    fig.tight_layout(pad=0.1)
    fig.savefig("time_filter_time.png", format='png', bbox_inches='tight')
    plt.show()


draw_vary_batch_size()
draw_vary_parallelism()
draw_breakdown()
draw_filter_time()
