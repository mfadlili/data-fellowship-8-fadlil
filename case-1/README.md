## How to run the program?
First, you need to create a virtual environment inside a folder using Command Prompt:

```sh
pip install virtualenv
python -m venv [your_virtual_env_name]
```

Next, activate the virtual environment:
```sh
.\[your_virtual_env_name]\Scripts\activate
```

Clone this repository to your folder using this command :
```sh
git clone git@github.com:mfadlili/df8-iykra-case1.git
```

Change the directory on cmd to the repository folder and run this command to install the dependencies:
```sh
cd .\df8-iykra-case1
pip install -r requirements.txt
```
Replace the contents of key.json with your own access key.

Don't forget to replace project_id, url and bucket with your own, here's the description:

project_id : project_id of the project name in your google cloud storage service

url : url of the file to download

bucket : bucket name on your google cloud storage service

Wait until the installation finished. Run the program by run the command below:
```sh
python main.py
```