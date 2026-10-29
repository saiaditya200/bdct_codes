import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

# 1. Load dataset
df = pd.read_csv("AirPassengers.csv")
print(df.head())
df.info()
print(df.isnull().sum())
print(df.describe(include="all"))

# 2. Prepare time series
date_col = "Month" if "Month" in df.columns else df.columns[0]
value_col = "Passengers" if "Passengers" in df.columns else (
    "#Passengers" if "#Passengers" in df.columns else df.columns[1]
)

df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
df = df.dropna(subset=[date_col, value_col]).sort_values(date_col)
df = df.set_index(date_col)
data = df[value_col].asfreq("MS").interpolate()

# 3. Train-test split: last 12 months are test data
if len(data) <= 12:
    raise ValueError("Dataset must contain more than 12 monthly observations.")
train = data.iloc[:-12]
test = data.iloc[-12:]

# 4. Build and fit ARIMA model
# statsmodels' current API is statsmodels.tsa.arima.model.ARIMA
model = ARIMA(train, order=(2, 1, 2))
model_fit = model.fit()
print(model_fit.summary())

# 5. Forecast the test period
forecast = model_fit.forecast(steps=len(test))
forecast.index = test.index

# 6. Plot training data, actual test data, and forecast
plt.figure(figsize=(10, 5))
plt.plot(train.index, train, label="Training Data")
plt.plot(test.index, test, label="Actual Data")
plt.plot(forecast.index, forecast, label="ARIMA Forecast")
plt.title("ARIMA Time Series Forecasting")
plt.xlabel("Month")
plt.ylabel("Passengers")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# 7. Optional: display actual and forecast values together
comparison = pd.DataFrame({"Actual": test, "Forecast": forecast})
print(comparison)
