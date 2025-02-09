"""
utilities for making nice tables in notebooks
"""
import pandas as pd

# variable name to readable name
var2read = {'ED_visits_frac': 'ED (overall)',
            'ED_hosp_frac': 'ED hospitalization (overall)',
            'UC_visits_frac': 'Urgent care (overall)',
            'Asthma_use_frac': 'Any asthma',
            'ED_visits_usefrac': 'ED (any asthma)',
            'ED_hosp_usefrac': 'ED hospitalization (any asthma)',
            'UC_visits_usefrac': 'Urgent care (any asthma)',
            'ED_hosp_visitfrac': 'ED hospitalization (had ED)'
            }


# table styles

def p_col_style(styler):
    """
    set style of column holding p-values
    - display up to 3 decimal places...
    - ...unless less than 0.001, in which case display < 0.001 TODO how
    """
    styler.format(precision=3, subset=['p'])
    return styler


# model fit result(s) -> dataframe of results

def ols2df(ols_res):
    """
    utility to generate a dataframe of results from a dict of sm.OLS model fit outputs

    ols_res: dict with str keys (name of dependent variable) and sm.OLS fit RegressionResults
    """
    dfs = []
    for col, res in ols_res.items():
        res_df = pd.DataFrame({"coef": res.params, "t": res.tvalues, "p": res.pvalues, "R2": res.rsquared,
                               "std err": res.bse})
        res_df["dep_var"] = col
        dfs.append(res_df)
    df = pd.concat(dfs)

    # reset index
    df.reset_index(inplace=True, names="indep_var")
    df.set_index(["dep_var", "indep_var"], inplace=True)

    return df

def spreg2df(spreg_res, name_attr="name_x"):
    """
    utility to generate a dataframe of results from a dict of spreg fit outputs

    spreg_res: dict with str keys (name of dependent variable) and spreg fit
    """
    dfs = []
    for col, res in spreg_res.items():
        res_df = pd.DataFrame({"coef": res.betas.flatten(), "std err": res.std_err.flatten(),
                               "p": [i[1] for i in res.z_stat]}, index=getattr(res, name_attr))
        res_df.drop("CONSTANT", inplace=True)
        res_df["dep_var"] = col
        dfs.append(res_df)
    df = pd.concat(dfs)

    # reset index
    df.reset_index(inplace=True, names="indep_var")
    df.set_index(["dep_var", "indep_var"], inplace=True)

    return df