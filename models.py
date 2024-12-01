from tortoise import fields
from tortoise.models import Model


class Tickers(Model):
    timestamp = fields.DatetimeField()
    symbol = fields.TextField()
    open = fields.FloatField()
    high = fields.FloatField()
    low = fields.FloatField()
    close = fields.FloatField()
    volume = fields.FloatField()

    def __dict__(self):
        return f"'id': {self.id}, 'date': {self.date}, 'symbol': {self.symbol}, 'open': {self.open}, 'high': {self.high},  'close': {self.close},  'volume': {self.volume}"


class Symbols(Model):
    symbol = fields.TextField()

    def __dict__(self):
        return f"'id': {self.id}, 'symbol': {self.symbol}"
