import matplotlib.pyplot as plt
import matplotlib.patches as patches

def create_spark_dag():
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis('off')

    # Define box style
    box_props = dict(boxstyle='round,pad=0.5', facecolor='#f0f0f0', edgecolor='black', linewidth=1.5)
    bbox_rdd = dict(boxstyle='round,pad=0.5', facecolor='#d9ead3', edgecolor='#6aa84f', linewidth=2)
    bbox_action = dict(boxstyle='round,pad=0.5', facecolor='#fff2cc', edgecolor='#d6b656', linewidth=2)

    # Function to draw arrows
    def draw_arrow(start, end):
        ax.annotate("", xy=end, xytext=start,
                    arrowprops=dict(arrowstyle="->", lw=1.5, color='black'))

    # Nodes
    # Input
    ax.text(5, 11, "Input Text Files\n(data/text/*/*.txt)", ha='center', va='center', bbox=box_props)
    
    # 1. wholeTextFiles
    draw_arrow((5, 10.5), (5, 9.5))
    ax.text(5, 9, "wholeTextFiles\nRDD[(path, content)]", ha='center', va='center', bbox=bbox_rdd)

    # 2. FlatMap
    draw_arrow((5, 8.5), (5, 7.5))
    ax.text(5, 7, "flatMapToPair\nTokenize, Filter Stopwords\nExtact Lang from Path\nRDD[(Lang, Word)]", ha='center', va='center', bbox=box_props)

    # Split for Total vs Unique
    draw_arrow((5, 6.3), (3, 5.5))
    draw_arrow((5, 6.3), (7, 5.5))

    # Branch A: Total
    ax.text(3, 5, "mapToPair(Lang, 1)\nreduceByKey(+)", ha='center', va='center', bbox=box_props)
    draw_arrow((3, 4.5), (3, 3.5))
    ax.text(3, 3, "Total Counts\nRDD[(Lang, Count)]", ha='center', va='center', bbox=bbox_rdd)

    # Branch B: Unique
    ax.text(7, 5, "distinct()\nmapToPair(Lang, 1)\nreduceByKey(+)", ha='center', va='center', bbox=box_props)
    draw_arrow((7, 4.5), (7, 3.5))
    ax.text(7, 3, "Unique Counts\nRDD[(Lang, Count)]", ha='center', va='center', bbox=bbox_rdd)

    # Join
    draw_arrow((3, 2.5), (5, 1.8))
    draw_arrow((7, 2.5), (5, 1.8))
    
    ax.text(5, 1.3, "JOIN -> Calculate TTR\ncollect() -> Save CSV", ha='center', va='center', bbox=bbox_action)

    plt.tight_layout()
    plt.savefig('latex/images/spark_flow_diagram.png', dpi=150, bbox_inches='tight')
    print("Saved latex/images/spark_flow_diagram.png")

if __name__ == "__main__":
    create_spark_dag()
