package com.itranswarp.exchange.match;

import com.itranswarp.exchange.bean.OrderBookBean;
import com.itranswarp.exchange.enums.Direction;
import com.itranswarp.exchange.enums.OrderStatus;
import com.itranswarp.exchange.model.trade.OrderEntity;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class MatchEngine {
    public final OrderBook buyBook = new OrderBook(Direction.BUY);
    public final OrderBook sellBook = new OrderBook(Direction.SELL);
    public BigDecimal marketPrice = BigDecimal.ZERO;//最新市场价
    public long sequenceId;//上一次处理的sequence id

    public MatchResult processOrder(long sequenceId, OrderEntity order) {
        switch (order.direction) {
            case BUY:
                return processOrder(sequenceId, order, this.sellBook, this.buyBook);
            case SELL:
                return processOrder(sequenceId, order, this.buyBook, this.sellBook);
            default:
                throw new IllegalArgumentException("Invalid direction");
        }
    }

    public OrderBookBean getOrderBook(int maxDepth) {
        return new OrderBookBean(this.sequenceId, this.marketPrice, this.buyBook.getOrderBook(maxDepth),
                this.sellBook.getOrderBook(maxDepth));
    }

    public void debug() {
        System.out.println("---------- match engine ----------");
        System.out.println(this.sellBook);
        System.out.println("  ----------");
        System.out.println("  " + this.marketPrice);
        System.out.println("  ----------");
        System.out.println(this.buyBook);
        System.out.println("---------- // match engine ----------");
    }


    private MatchResult processOrder(long sequenceId, OrderEntity takerOrder, OrderBook makerBook, OrderBook anotherBook)
    {
        this.sequenceId = sequenceId;
        long ts = takerOrder.createdAt;
        MatchResult matchResult = new MatchResult(takerOrder);
        BigDecimal takerUnfilledQuantity = takerOrder.quantity;
        for (; ; ) {
            OrderEntity makerOrder = makerBook.getFirst();
            if (makerOrder == null) {
                //对手盘不存在
                break;
            }
            if(takerOrder.direction==Direction.BUY&&takerOrder.price.compareTo(makerOrder.price)<0){
                //买入价格比卖盘第一档价格低
                break;
            }else if(takerOrder.direction==Direction.SELL&&takerOrder.price.compareTo(makerOrder.price)>0){
                //卖单价格比买盘第一档价格高
                break;
            }
            this.marketPrice=makerOrder.price;
            BigDecimal matchedQuantity = takerUnfilledQuantity.min(makerOrder.unfilledQuantity);
            matchResult.add(makerOrder.price, matchedQuantity,makerOrder);
            takerUnfilledQuantity=takerUnfilledQuantity.subtract(matchedQuantity);
            BigDecimal makerUnfilledQuantity = makerOrder.unfilledQuantity.subtract(matchedQuantity);
            if(makerUnfilledQuantity.signum()==0){//对手盘完全成交
                makerOrder.updateOrder(makerUnfilledQuantity, OrderStatus.FULLY_FILLED, ts);
                makerBook.remove(makerOrder);
            }else {
                //对手盘部分成交
                makerOrder.updateOrder(makerUnfilledQuantity, OrderStatus.PARTIAL_FILLED,ts);
            }
            //Taker订单完全成交后，退出循环
            if(takerUnfilledQuantity.signum()==0){
                takerOrder.updateOrder(takerUnfilledQuantity,OrderStatus.FULLY_FILLED,ts);
                break;
            }
        }
            //Taker订单未完全成交时，放入丁单薄
            if(takerUnfilledQuantity.signum()>0){
                takerOrder.updateOrder(takerUnfilledQuantity,takerUnfilledQuantity.compareTo(takerOrder.quantity)==0
                        ?OrderStatus.PENDING:OrderStatus.PARTIAL_FILLED,ts);
                anotherBook.add(takerOrder);
            }
        return matchResult;
    }
}
























