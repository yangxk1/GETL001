import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import seaborn as sns

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 设置绘图风格
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)

def load_data(csv_file='performance_data.csv'):
    """加载CSV数据"""
    df = pd.read_csv(csv_file)
    return df

def plot_box_plots(df):
    """绘制箱形图对比时间和内存，并叠加平均值柱形图"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # 准备时间数据
    time_data = []
    time_labels = []
    time_avg = []
    for _, row in df.iterrows():
        time_data.append([row['Min Time (ms)'], row['Average Time (ms)'], row['Max Time (ms)']])
        time_labels.append(row['Info'])
        time_avg.append(row['Average Time (ms)'])

    # 准备内存数据
    memory_data = []
    memory_labels = []
    memory_avg = []
    for _, row in df.iterrows():
        memory_data.append([row['Min Memory (B)'] / (1024*1024),
                           row['Average Memory (B)'] / (1024*1024),
                           row['Max Memory (B)'] / (1024*1024)])
        memory_labels.append(row['Info'])
        memory_avg.append(row['Average Memory (B)'] / (1024*1024))

    # 绘制时间箱形图
    positions1 = np.arange(1, len(time_data) + 1)
    bp1 = axes[0].boxplot(time_data, positions=positions1, tick_labels=time_labels,
                          patch_artist=True, widths=0.5)

    # 在箱形图上叠加平均值柱形图
    bar_width = 0.3
    bars1 = axes[0].bar(positions1, time_avg, width=bar_width, alpha=0.6,
                        color='gold', edgecolor='orange', linewidth=2,
                        label='Average', zorder=5)

    # 在柱形图上添加数值标签
    for i, (pos, avg) in enumerate(zip(positions1, time_avg)):
        axes[0].text(pos, avg, f'{int(avg)}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold', color='darkred')

    axes[0].set_title('Time Performance Comparison (Box Plot with Avg Bar)',
                     fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Operation', fontsize=12)
    axes[0].set_ylabel('Time (ms)', fontsize=12)
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc='upper left')

    # 设置箱形图颜色
    colors = plt.cm.Set3(np.linspace(0, 1, len(time_data)))
    for patch, color in zip(bp1['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    # 绘制内存箱形图
    positions2 = np.arange(1, len(memory_data) + 1)
    bp2 = axes[1].boxplot(memory_data, positions=positions2, tick_labels=memory_labels,
                          patch_artist=True, widths=0.5)

    # 在箱形图上叠加平均值柱形图
    bars2 = axes[1].bar(positions2, memory_avg, width=bar_width, alpha=0.6,
                        color='lightcoral', edgecolor='red', linewidth=2,
                        label='Average', zorder=5)

    # 在柱形图上添加数值标签
    for i, (pos, avg) in enumerate(zip(positions2, memory_avg)):
        axes[1].text(pos, avg, f'{int(avg)}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold', color='darkblue')

    axes[1].set_title('Memory Performance Comparison (Box Plot with Avg Bar)',
                     fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Operation', fontsize=12)
    axes[1].set_ylabel('Memory (MB)', fontsize=12)
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc='upper left')

    # 设置箱形图颜色
    for patch, color in zip(bp2['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    plt.tight_layout()
    plt.savefig('performance_box_plots.png', dpi=300, bbox_inches='tight')
    print("箱形图已保存为: performance_box_plots.png")
    plt.close()

def main(arg=None):
    """主函数"""
    print("=" * 60)
    print("Performance Data Visualization Tool")
    print("=" * 60)

    # 获取CSV文件路径
    if len(sys.argv) < 2:
        csv_file = 'performance_data.csv'
        print(f"\n未指定CSV文件，使用默认文件: {csv_file}")
    else:
        csv_file = sys.argv[1]
        print(f"\n使用指定CSV文件: {csv_file}")

    # 加载数据
    df = load_data(csv_file)
    print("\n数据加载成功!")
    print(df.to_string(index=False))
    print("\n" + "=" * 60)

    # 生成各种图表
    print("\n正在生成箱形图...")
    plot_box_plots(df)

    print("\n" + "=" * 60)
    print("所有图表生成完成!")
    print("=" * 60)

if __name__ == "__main__":
    main()

