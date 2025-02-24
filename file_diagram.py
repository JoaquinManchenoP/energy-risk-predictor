import os
from graphviz import Digraph

def create_visual_file_structure(root_dir):
    dot = Digraph(comment='File Structure', format='png', 
                  graph_attr={'rankdir': 'LR', 'bgcolor': 'white', 'nodesep': '0.5', 'ranksep': '1.0'},
                  node_attr={'style': 'filled', 'fillcolor': '#DDEEFF', 'shape': 'box', 'fontname': 'Arial', 'width': '1.5', 'height': '0.5'})

    created_nodes = set()

    for foldername, subfolders, filenames in os.walk(root_dir):
        parent = foldername.replace(root_dir, "").strip("/").strip("\\") or "Root"
        if parent not in created_nodes:
            dot.node(parent, parent, fillcolor='#66B2FF', style='filled', shape='folder')
            created_nodes.add(parent)

        for subfolder in subfolders:
            subfolder_path = os.path.join(foldername, subfolder).replace(root_dir, "").strip("/").strip("\\")
            if subfolder_path not in created_nodes:
                dot.node(subfolder_path, subfolder, fillcolor='#99CCFF', shape='folder', width='1.8')
                created_nodes.add(subfolder_path)
            dot.edge(parent, subfolder_path)

        for filename in filenames:
            file_path = os.path.join(foldername, filename).replace(root_dir, "").strip("/").strip("\\")
            dot.node(file_path, filename, fillcolor='#FFDD99', shape='note', width='1.5')  # Use "note" shape for files
            dot.edge(parent, file_path)

    return dot

# Set the root directory (current directory by default)
root_directory = "."
diagram = create_visual_file_structure(root_directory)

# Save as PNG
diagram.render("file_structure", cleanup=True)

print("✅ File structure diagram saved as 'file_structure.png'.")
