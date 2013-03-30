Play with Glacier.

- Motivation.
Initial motivation is to delete an abandoned vault without archive id.
I have used Arq to backup my private data into Glacier.
Arq is very usuful for me.
But if I remove a directory from the backup source set, all backuped data in AWS deleted exclude vault.
And the vault will remain without any archive id.
To delete the vault, firstly I must delete all achives in it and make it empty.

- Prerequisites.
Python 2.7.1 or later.
boto 2.8.0 or later.
