import os

def move_text_files(source_path="G:\\FlyCast\\source\\", sink_path="G:\\FlyCast\\sink\\"):
    for files in os.listdir(source_path):
        print(files)
        os.rename(source_path+files, sink_path+files)
