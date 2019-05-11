package com.singleton.trades.processor;

import static java.lang.Math.abs;
import static java.lang.Math.signum;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import stock.RichTrade;
import stock.Trade;

public class TradeProcessor implements Processor<Long, Trade> {

  private KeyValueStore<Integer, Double> amountCounter;
  private KeyValueStore<Integer, Double> averageCounter;

  private ProcessorContext context;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    amountCounter = (KeyValueStore<Integer, Double>) context.getStateStore("AmountCounter");
    averageCounter = (KeyValueStore<Integer, Double>) context.getStateStore("AverageCounter");
    this.context = context;
  }

  @Override
  public void process(Long key, Trade value) {
    // TODO: determine position Id

    // Gather Rich trade
    final var richTradeBuilder = RichTrade
        .newBuilder()
        .setValue(value.getAmount() * value.getPrice())
        .setPrice(value.getPrice())
        .setDateTime(value.getDateTime())
        .setCurrency(value.getCurrency())
        .setTicker(value.getTicker())
        .setAmount(value.getAmount())
        .setAccount(value.getAccount());

    final var compositeKey = (key.toString() + value.getTicker()).hashCode();

    var currentAmount = amountCounter.get(compositeKey);
    var currentAverage = averageCounter.get(compositeKey); // Current average needs to calculate
    // PnL

    if (currentAmount == null) {
      currentAmount = 0D;
    }

    amountCounter.put(compositeKey, currentAmount + value.getAmount());

    if (currentAverage == null) {
      currentAverage = 0D;
    }

    if (signum(currentAmount) == signum(value.getAmount()) || signum(currentAmount) == 0) {
      currentAverage = (
          currentAmount * currentAverage + value.getAmount() * value.getPrice()
      ) / (currentAmount + value.getAmount());

      averageCounter.put(compositeKey, currentAverage);
    }

    if (signum(currentAmount) != signum(value.getAmount())) {
      // Fix pnl
      final var pnl = (value.getPrice() - currentAverage) * abs(value.getAmount());
      richTradeBuilder.setPnl(pnl);
    } else {
      richTradeBuilder.setPnl(0D);
    }

    context.forward(key, richTradeBuilder.build());
  }

  @Override
  public void close() {
    // Nothing to do
  }
}
