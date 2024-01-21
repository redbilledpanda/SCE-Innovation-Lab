#!/usr/bin/bash
set -x

BLUE='\033[0;34m'
RED='\033[0;31m'
GREEN='\033[0;32m'
if [ -f "/etc/debian_version" ]; then
  printf "\n${GREEN}This is debian OS"
  ##################################
  #### # SETTING VS CODE VENV # ####
  ##################################
  # step-00 - pre-requisite: create a virtual environment
  printf "\n${BLUE}Updating apt-get\n"; sleep 2
  sudo apt update > /dev/null 2>&1
  read -r -p "\nAre you running this project in vs code? [y/N]" response
  if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]
  then
      printf "\n${BLUE}Installing python3.10 virtual environment\n"; sleep 2
      sudo apt install python3-venv > /dev/null 2>&1
      printf "\n${BLUE}Creating/Using local virtual environment at \n"; sleep 2

      # store the home dir
      user_home_dir=$HOME
      # Set the path to the virtual environment
      venv_path=$HOME/Documents/PyProjects/SparkvEnv

      # Check if the virtual environment exists
      if [ ! -d "$venv_path" ]; then
          echo "Creating virtual environment..."
          python3 -m venv $venv_path
      fi

      # Activate the virtual environment
      source $venv_path/bin/activate

      ##########################################
      #### # INSTALLING REQUIRED PRE_REQS # ####
      ##########################################
      # installing python pip packages
      printf "\n${BLUE}Installating python pip packages needed\n"; sleep 2
      pip install -r requirements/pip_requirements.txt > /dev/null 2>&1
      # installing vs code extensions
      printf "\n${BLUE}Installating vs code extensions\n"; sleep 2
      while IFS="" read -r p || [ -n "$p" ]
      do
        code --install-extension "$p"
      done < requirements/vscode_requirements.txt
      #############################
      ### # INSTALLTING SPARK # ###
      #############################
      # step-01 - 1 - installing jdk
      printf "\n${BLUE}Installating latest (default) jdk / jre needed for spark\n"; sleep 2
      cd $HOME
      sudo apt install -y default-jre default-jdk > /dev/null 2>&1
      # step-01 - 2 - check java version
      printf "\n${BLUE}java version check, if this returns value, java is detected and working!\n"; sleep 2
      java -version
      # step-02 - 1 - check if we have a PySpark

      # Check if PySpark is installed in the virtual environment
      python3 -m pip show pyspark > /dev/null 2>&1

      if [ $? -eq 0 ]; then
          echo "PySpark is installed in the virtual environment."
      else
          echo "Installing PySpark in the virtual environment."
          python3 -m pip install pyspark
      fi

      # step-04 - install FindSpark package
      printf "\n${BLUE}Installing python3 pip 'findspark' package\n"; sleep 1
      python3 -m pip install findspark > /dev/null 2>&1
      
      # Deactivate the virtual environment
      deactivate
  else
      printf "\n${RED}The installation steps are only for vs code setup\n"
      exit 1;
  fi
  
else
   printf "\n${RED}This script is only for debian OS\n"
   exit 1;
fi
