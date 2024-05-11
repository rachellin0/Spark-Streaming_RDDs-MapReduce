def dfTask1(data):
    rdd = data.rdd if hasattr(data, 'rdd') else data
    if rdd.count()>10000:
        raise Exception('`outputTask1` has too many rows')
    rows = map(lambda x: (x[0], x[1], int(x[2])), rdd.collect())
    return pd.DataFrame(data=rows, columns=['Item Name','Price ($)','% Food Insecurity'])

def plotTask1(data, figsize=(8,8)):
    itemNames = pd.read_csv('keyfood_sample_items.csv')['Item Name']
    itemKey = dict(map(reversed,enumerate(itemNames)))
    df = dfTask1(data).sort_values(
        by = ['Item Name', '% Food Insecurity'],
        key = lambda x: list(map(lambda y: itemKey.get(y,y), x)))
    plt.figure(figsize=figsize)
    ax = sns.violinplot(x="Price ($)", y="Item Name", data=df, linewidth=0,
                        color='#ddd', scale='width', width=0.95)
    idx = len(ax.collections)
    sns.scatterplot(x="Price ($)", y="Item Name", hue='% Food Insecurity', data=df,
                    s=24, linewidth=0.5, edgecolor='gray', palette='YlOrRd')
    for h in ax.legend_.legendHandles:
        h.set_edgecolor('gray')
    pts = ax.collections[idx]
    pts.set_offsets(pts.get_offsets() + np.c_[np.zeros(len(df)),
                                            np.random.uniform(-.1, .1, len(df))])
    ax.set_xlim(left=0)
    ax.xaxis.grid(color='#eee')
    ax.yaxis.grid(color='#999')
    ax.set_title('Item Prices across KeyFood Stores in NYC')
    return ax

if 'outputTask1' not in locals():
    raise Exception('There is no `outputTask1` produced in Task 1.A')

plotTask1(outputTask1);