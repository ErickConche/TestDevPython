try{
    python --version

} catch {
    winget install python.python.3.10

}

python -m pip install venv
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install prefect
python -m pip install pandas
python -m pip install selenium
python -m pip install sqlite
 

Start-Process powershell  {prefect --no-prompt deploy --all
                           prefect server start;
                           
                          } 
Start-Process powershell  {Start-Sleep 2;  
                           prefect work-pool create --type process default
                           prefect worker start --pool default;
                          }