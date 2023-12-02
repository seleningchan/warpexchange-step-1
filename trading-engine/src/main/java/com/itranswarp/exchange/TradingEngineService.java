package com.itranswarp.exchange;

import com.itranswarp.exchange.assets.Asset;
import com.itranswarp.exchange.assets.AssetService;
import com.itranswarp.exchange.bean.OrderBookBean;
import com.itranswarp.exchange.clearing.ClearingService;
import com.itranswarp.exchange.enums.AssetEnum;
import com.itranswarp.exchange.enums.UserType;
import com.itranswarp.exchange.match.MatchDetailRecord;
import com.itranswarp.exchange.match.MatchEngine;
import com.itranswarp.exchange.match.MatchResult;
import com.itranswarp.exchange.message.event.AbstractEvent;
import com.itranswarp.exchange.message.event.OrderCancelEvent;
import com.itranswarp.exchange.message.event.OrderRequestEvent;
import com.itranswarp.exchange.message.event.TransferEvent;
import com.itranswarp.exchange.model.trade.OrderEntity;
import com.itranswarp.exchange.order.OrderService;
import com.itranswarp.exchange.store.StoreService;
import com.itranswarp.exchange.support.LoggerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map.Entry;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

@Component
public class TradingEngineService extends LoggerSupport {
    @Value("#{exchangeConfiguration.orderBookDepth}")
    int orderBookDepth=100;
    @Value("#{exchangeConfiguration.debugMode}")
    boolean debugMode = false;
    private long lastSequenceId = 0;
    private OrderBookBean latestOrderBook = null;
    private Queue<List<OrderEntity>> orderQueue = new ConcurrentLinkedQueue<>();
    private boolean orderBookChanged = false;

    boolean fatalError = false;
    @Autowired
    AssetService assetService;
    @Autowired
    OrderService orderService;
    @Autowired
    MatchEngine matchEngine;
    @Autowired
    ClearingService clearingService;
    @Autowired
    StoreService storeService;
    @Autowired(required = false)
    ZoneId zoneId = ZoneId.systemDefault();
    void processMessage(List<AbstractEvent> messages){
        /*for (AbstractEvent message : messages){
            processEvent(message);
        }*/
        this.orderBookChanged = false;
        for (AbstractEvent message : messages) {
            processEvent(message);
        }
        if (this.orderBookChanged) {
            // 获取最新的OrderBook快照:
            this.latestOrderBook = this.matchEngine.getOrderBook(this.orderBookDepth);
        }
    }
    private void panic() {
        logger.error("application panic. exit now...");
        this.fatalError = true;
        System.exit(1);
    }
    public void processEvent(AbstractEvent event){
        if(this.fatalError){
            return;
        }
        if(event.sequenceId<this.lastSequenceId){
            logger.warn("skip duplicate event: {}", event);
            return;
        }
        if(event.previousId>this.lastSequenceId){
            logger.warn("event lost: expected previous id {} but actual {} for event {}", this.lastSequenceId,
                    event.previousId, event);
            var events = this.storeService.loadEventsFromDb(this.lastSequenceId);
            if(events.isEmpty()){
                logger.error("cannot load lost event from db.");
                panic();
                return;
            }
            for (var e : events){
                this.processEvent(e);
            }
            return;
        }
        if (event.previousId != lastSequenceId) {
            logger.error("bad event: expected previous id {} but actual {} for event: {}", this.lastSequenceId,
                    event.previousId, event);
            panic();
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("process event {} -> {}: {}...", this.lastSequenceId, event.sequenceId, event);
        }
        if(event instanceof OrderRequestEvent){
            createOrder((OrderRequestEvent)event);
        }else if(event instanceof OrderCancelEvent){
            cancelOrder((OrderCancelEvent)event);
        }else if(event instanceof TransferEvent){
            transfer((TransferEvent)event);
        }
        this.lastSequenceId = event.sequenceId;
        if (logger.isDebugEnabled()) {
            logger.debug("set last processed sequence id: {}...", this.lastSequenceId);
        }
        if (debugMode) {
            this.validate();
            this.debug();
        }
    }
    void createOrder(OrderRequestEvent event){
        ZonedDateTime zdt = Instant.ofEpochMilli(event.createdAt).atZone(zoneId);
        int year = zdt.getYear();
        int month = zdt.getMonth().getValue();
        long orderId=event.sequenceId*1000+(year*100+month);
        OrderEntity order = orderService.createOrder(event.sequenceId,event.createdAt,orderId,event.userId,event.direction,event.price,event.quantity);
        if(order==null){
            logger.warn("create order failed");
            return;
        }
        MatchResult result = matchEngine.processOrder(event.sequenceId,order);
        clearingService.clearMatchResult(result);
        if(!result.matchDetails.isEmpty()){
            List<OrderEntity> closedOrders = new ArrayList<>();
            if(result.takerOrder.status.isFInalStatus){
                closedOrders.add(result.takerOrder);
            }
            for (MatchDetailRecord detail : result.matchDetails){
                var maker = detail.makerOrder();
                if(maker.status.isFInalStatus){
                    closedOrders.add(maker);
                }
            }
            this.orderQueue.add(closedOrders);
        }
    }
    void saveOrders(){

    }
    void cancelOrder(OrderCancelEvent orderCancelEvent){

    }
    void transfer(TransferEvent event){

    }


    public void debug() {
        System.out.println("========== trading engine ==========");
        this.assetService.debug();
        this.orderService.debug();
        this.matchEngine.debug();
        System.out.println("========== // trading engine ==========");
    }

    void validate() {
        logger.debug("start validate...");
        validateAssets();
        validateOrders();
        validateMatchEngine();
        logger.debug("validate ok.");
    }

    void validateAssets() {
        // 验证系统资产完整性:
        BigDecimal totalUSD = BigDecimal.ZERO;
        BigDecimal totalBTC = BigDecimal.ZERO;
        for (Entry<Long, ConcurrentMap<AssetEnum, Asset>> userEntry : this.assetService.getUserAssets().entrySet()) {
            Long userId = userEntry.getKey();
            ConcurrentMap<AssetEnum, Asset> assets = userEntry.getValue();
            for (Entry<AssetEnum, Asset> entry : assets.entrySet()) {
                AssetEnum assetId = entry.getKey();
                Asset asset = entry.getValue();
                if (userId.longValue() == UserType.DEBT.getInternalUserId()) {
                    // 系统负债账户available不允许为正:
                    require(asset.getAvailable().signum() <= 0, "Debt has positive available: " + asset);
                    // 系统负债账户frozen必须为0:
                    require(asset.getFrozen().signum() == 0, "Debt has non-zero frozen: " + asset);
                } else {
                    // 交易用户的available/frozen不允许为负数:
                    require(asset.getAvailable().signum() >= 0, "Trader has negative available: " + asset);
                    require(asset.getFrozen().signum() >= 0, "Trader has negative frozen: " + asset);
                }
                switch (assetId) {
                    case USD -> {
                        totalUSD = totalUSD.add(asset.getTotal());
                    }
                    case BTC -> {
                        totalBTC = totalBTC.add(asset.getTotal());
                    }
                    default -> require(false, "Unexpected asset id: " + assetId);
                }
            }
        }
        // 各类别资产总额为0:
        require(totalUSD.signum() == 0, "Non zero USD balance: " + totalUSD);
        require(totalBTC.signum() == 0, "Non zero BTC balance: " + totalBTC);
    }

    void validateOrders() {
        // 验证订单:
        Map<Long, Map<AssetEnum, BigDecimal>> userOrderFrozen = new HashMap<>();
        for (Entry<Long, OrderEntity> entry : this.orderService.getActiveOrders().entrySet()) {
            OrderEntity order = entry.getValue();
            require(order.unfilledQuantity.signum() > 0, "Active order must have positive unfilled amount: " + order);
            switch (order.direction) {
                case BUY -> {
                    // 订单必须在MatchEngine中:
                    require(this.matchEngine.buyBook.exist(order), "order not found in buy book: " + order);
                    // 累计冻结的USD:
                    userOrderFrozen.putIfAbsent(order.userId, new HashMap<>());
                    Map<AssetEnum, BigDecimal> frozenAssets = userOrderFrozen.get(order.userId);
                    frozenAssets.putIfAbsent(AssetEnum.USD, BigDecimal.ZERO);
                    BigDecimal frozen = frozenAssets.get(AssetEnum.USD);
                    frozenAssets.put(AssetEnum.USD, frozen.add(order.price.multiply(order.unfilledQuantity)));
                }
                case SELL -> {
                    // 订单必须在MatchEngine中:
                    require(this.matchEngine.sellBook.exist(order), "order not found in sell book: " + order);
                    // 累计冻结的BTC:
                    userOrderFrozen.putIfAbsent(order.userId, new HashMap<>());
                    Map<AssetEnum, BigDecimal> frozenAssets = userOrderFrozen.get(order.userId);
                    frozenAssets.putIfAbsent(AssetEnum.BTC, BigDecimal.ZERO);
                    BigDecimal frozen = frozenAssets.get(AssetEnum.BTC);
                    frozenAssets.put(AssetEnum.BTC, frozen.add(order.unfilledQuantity));
                }
                default -> require(false, "Unexpected order direction: " + order.direction);
            }
        }
        // 订单冻结的累计金额必须和Asset冻结一致:
        for (Entry<Long, ConcurrentMap<AssetEnum, Asset>> userEntry : this.assetService.getUserAssets().entrySet()) {
            Long userId = userEntry.getKey();
            ConcurrentMap<AssetEnum, Asset> assets = userEntry.getValue();
            for (Entry<AssetEnum, Asset> entry : assets.entrySet()) {
                AssetEnum assetId = entry.getKey();
                Asset asset = entry.getValue();
                if (asset.getFrozen().signum() > 0) {
                    Map<AssetEnum, BigDecimal> orderFrozen = userOrderFrozen.get(userId);
                    require(orderFrozen != null, "No order frozen found for user: " + userId + ", asset: " + asset);
                    BigDecimal frozen = orderFrozen.get(assetId);
                    require(frozen != null, "No order frozen found for asset: " + asset);
                    require(frozen.compareTo(asset.getFrozen()) == 0,
                            "Order frozen " + frozen + " is not equals to asset frozen: " + asset);
                    // 从userOrderFrozen中删除已验证的Asset数据:
                    orderFrozen.remove(assetId);
                }
            }
        }
        // userOrderFrozen不存在未验证的Asset数据:
        for (Entry<Long, Map<AssetEnum, BigDecimal>> userEntry : userOrderFrozen.entrySet()) {
            Long userId = userEntry.getKey();
            Map<AssetEnum, BigDecimal> frozenAssets = userEntry.getValue();
            require(frozenAssets.isEmpty(), "User " + userId + " has unexpected frozen for order: " + frozenAssets);
        }
    }

    void validateMatchEngine() {
        // OrderBook的Order必须在ActiveOrders中:
        Map<Long, OrderEntity> copyOfActiveOrders = new HashMap<>(this.orderService.getActiveOrders());
        for (OrderEntity order : this.matchEngine.buyBook.book.values()) {
            require(copyOfActiveOrders.remove(order.id) == order,
                    "Order in buy book is not in active orders: " + order);
        }
        for (OrderEntity order : this.matchEngine.sellBook.book.values()) {
            require(copyOfActiveOrders.remove(order.id) == order,
                    "Order in sell book is not in active orders: " + order);
        }
        // activeOrders的所有Order必须在Order Book中:
        require(copyOfActiveOrders.isEmpty(), "Not all active orders are in order book.");
    }

    void require(boolean condition, String errorMessage) {
        if (!condition) {
            logger.error("validate failed: {}", errorMessage);
            panic();
        }
    }



























































}
