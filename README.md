## covid-uk-dashboard
python3 code to export .pngs for (public) English covid-19 data, national and regional.

![England](example/EnglandKeyData.png)

```python
python src/covid_chart_generator.py
```
Output .png charts will be saved to `charts` folder.

Various useful modules to build dataframes on specific metrics in `src/modules/dataframe_builder.py`

### Requirements
Python > 3.6  
pandas  
numpy  
matplotlib  
seaborn  
[gcsfs](https://gcsfs.readthedocs.io/en/latest/) (for reading [Zoe](https://covid.joinzoe.com/) data)  
[uk-covid19] (https://pypi.org/project/uk-covid19/) (for accessing UKHSA API)

I'd recommend setting up an environment (easiest with miniconda or similar), then just `pip install pandas numpy matplotlib seaborn gcsfs uk-covid19`