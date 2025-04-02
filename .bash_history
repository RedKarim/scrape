sudo dnf install gcc zlib-devel bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel xz-devel
sudo dnf install -y git
curl https://pyenv.run | bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
pyenv install -l | grep '3.11\.'
pyenv install 3.11.5
pyenv global 3.11.5
pyenv rehash
pyenv versions
sudo vi /etc/yum.repos.d/google-chrome.repo
sudo dnf install -y google-chrome-stable
google-chrome --version
pip install chromedriver-binary==131.0.6778.69.0
sudo dnf install tmux
cd scrape-with-gemini/
ls
python3 run.py 
pip install selenium
pip install spacy
pip install bs4
python3 run.py 
pip install google.generativeai
python3 run.py 
python3 -m spacy download ja_core_news_sm
python3 run.py 
tmux new -s scrape
ls
python3 run.py 
tmux exit
exit
tmux ls
tmux a
tmux ls
tmux a
cd HP_scrape/
ls
tmux new -s hp_scrape
tmux ls
tmux new -s gemini
ls
python3 company_HP_scrape.py 
exit
tmux a
tmux ls
tmux a -t gemini
tmux a -t hp_scrape
tmux ls
cd scrape-with-gemini/
ls
python3 run.py 
sudo dnf install -y google-chrome-stableexit
exit
tmux a gemini
tmux a
exit
tmux
tmux new -s gemini
cd scrape-with-gemini/
ls
python3 run.py 
exit
tmux a
tmux new -s gemini
tmux a
ls
cd scrape-with-gemini/
python3 run.py 
exit
tmux new -s executives
tmux a
tmux ls
tmux a
ls
cd scrape-with-gemini/
ls
python3 run.py 
ls
cd scrape-with-gemini-executives/
ls
tmux ls
tmux new -s kojima
tmux a
/bin/python3 /home/ec2-user/.cursor-server/extensions/ms-python.python-2024.12.3-linux-x64/python_files/printEnvVariablesToFile.py /home/ec2-user/.cursor-server/extensions/ms-python.python-2024.12.3-linux-x64/python_files/deactivate/bash/envVars.txt
/usr/bin/python3 /home/ec2-user/.cursor-server/extensions/ms-python.python-2024.12.3-linux-x64/python_files/printEnvVariablesToFile.py /home/ec2-user/.cursor-server/extensions/ms-python.python-2024.12.3-linux-x64/python_files/deactivate/bash/envVars.txt
tmux ls
python3 run.py
tmux new -s kojima2
python3 scrape-with-gemini-executives/run.py
mkdir data
python3 scrape-with-gemini-executives/run.py
cd scrape-with-gemini-executives
python3 run.py
cd scrape-with-gemini-executives/
python3 run.py
pwd
python3 run.py
sftp -i "C:/path/to/kojima_japan.pem" ec2-user@43.207.208.171
ls
cd scrape-with-gemini-executives
ls
cat README.md 
cat run.py
ls
cat data
cat sales_scraper.log
ls
python run.py
python3 run.py
pythin3 run.py
python3 run.py
cd scrape-with-gemini-executives/
python3 run.py
cd
ls
