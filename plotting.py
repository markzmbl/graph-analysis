import matplotlib
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import LinearSegmentedColormap, to_rgba, to_rgb

# Define the color stops
white_color = to_rgb("#ffffff")
primary_color = to_rgb("#00355a")
complementary_color = to_rgb("#fc8d62")  # Coral

# Hardcoded Matplotlib Set2 colors
set2_colors = [
    to_rgb("#a6d854"),  # Lime Green
    to_rgb("#ffd92f"),  # Yellow
    to_rgb("#e78ac3"),  # Pink
    to_rgb("#66c2a5"),  # Green
    to_rgb("#e5c494")   # Tan
]

primary_palette = LinearSegmentedColormap.from_list(
    "uni_vienna_blue", [white_color, primary_color], N=256)

interpolation_point = 0.35  # Move towards a lighter shade of blue
secondary_color = list(to_rgba(primary_palette(interpolation_point)))

# Adjust Red and Green components to enhance the bluish tint
secondary_color[0] *= 0.75  # Reduce red
secondary_color[1] *= 0.85  # Reduce green

complementary_palette = LinearSegmentedColormap.from_list(
    "competitor_orange", [white_color, complementary_color], N=256)

diverging_palette = LinearSegmentedColormap.from_list(
    "diverging_blue_orange",
    [complementary_color, white_color, primary_color], N=256
)

a = .75
b = .5
pus_palette = matplotlib.colormaps["cmap"]
intermediate_palette = LinearSegmentedColormap.from_list("intermediate_palette", [primary_palette(a), pus_palette(b)])
# Create new colormap
my_pus_palette = LinearSegmentedColormap.from_list(
    "perceptually_continuous", [
        *(primary_palette(i) for i in np.linspace(1, a, int((1-a)*256))),
        *(intermediate_palette(i) for i in np.linspace(0, 1, int((a-b)*256))),
        *(pus_palette(i) for i in np.linspace(b, 1, int((1-b)*256)))
    ]
)

def plot_color_gradients(ax, cmap, title):
    gradient = np.linspace(0, 1, 256).reshape(1, -1)
    ax.imshow(gradient, aspect="auto", cmap=cmap)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title, fontsize=12)


def plot_color_swatches(ax, colors, title):
    n = len(colors)
    for i, color in enumerate(colors):
        ax.add_patch(plt.Rectangle((i, 0), 1, 1, color=color))
    ax.set_xlim(0, n)
    ax.set_ylim(0, 1)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title, fontsize=12)


if __name__ == "__main__":
    fig, axes = plt.subplots(4, 1, figsize=(8, 6))

    # Plot gradients
    plot_color_gradients(axes[0], primary_palette, "Primary Palette")
    plot_color_gradients(axes[1], complementary_palette, "Complementary Palette")
    plot_color_gradients(axes[2], diverging_palette, "Diverging Palette")
    plot_color_gradients(axes[3], my_pus_palette, "Perceptually Uniform Sequential Palette")


    plt.tight_layout()
    plt.show()

    fig, axes = plt.subplots(1, 2, figsize=(8, 2))


    # Plot Key Colors (Primary, Complementary, Secondary)
    plot_color_swatches(axes[0], [primary_color, secondary_color, complementary_color], "Key Colors")

    # Plot Set2 swatches
    plot_color_swatches(axes[1], set2_colors, "additional")

    plt.tight_layout()
    plt.show()