import os

data_dir = "./playlist"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


file_path = os.path.join(data_dir, "playlist_id.txt")


playlist_id = input("Bitte gib die Playlist-ID ein: ")


try:
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(playlist_id)
    print(f"Die Playlist-ID wurde erfolgreich in der Datei gespeichert: {file_path}")
except Exception as e:
    print(f"Es ist ein Fehler aufgetreten: {e}")
