from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    #Your code here

    #Define Order Object
    order_object = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'], buy_currency=order['buy_currency'], sell_currency=order['sell_currency'], buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])

    if 'creator_id' in order.keys():
        #CASE when Creator Id exits
        order_object.creator_id = order['creator_id']
    order = order_object
    #Add to Session
    session.add(order)
    #Commit ot session
    session.commit()

    orders = session.query(Order).filter(Order.buy_currency == order.sell_currency).filter(Order.sell_currency == order.buy_currency).filter(Order.filled == None).all()
    market_rate = 0.0
    id = None

    for item in orders:
        #check each item in orders
        #print(market_rate)
        new_market_rate = item.sell_amount / item.buy_amount
        #print(new_market_rate)

        if new_market_rate < market_rate:
            continue
        elif new_market_rate == market_rate:
            market_rate = new_market_rate
        elif new_market_rate > market_rate:
            id = item.id
            #Switch rate
            market_rate = new_market_rate
        else:
            print("Check market rate")

    if ((market_rate >= order.buy_amount / order.sell_amount) and (id != -1)):

        market_fill_date = datetime.now()

        partyOne = session.query(Order).get(id)
        #Get order
        order = session.query(Order).get(order.id)

        partyOne.filled = market_fill_date
        order.filled = market_fill_date

        order.counterparty_id = id
        partyOne.counterparty_id = order.id

        #Commit to Session
        session.commit()

        #define Transaction dictionary
        transaction = {}

        if partyOne.buy_amount > order.sell_amount:
            #ID
            transaction['creator_id'] = partyOne.id
            #Sending
            transaction['receiver_pk'] = partyOne.receiver_pk
            transaction['sender_pk'] = partyOne.sender_pk
            #Sell Currency
            transaction['sell_currency'] = partyOne.sell_currency
            transaction['buy_currency'] = partyOne.buy_currency
            #Buy Amount
            amount_bought = partyOne.buy_amount - order.sell_amount
            amount_sold = partyOne.sell_amount - order.buy_amount
            transaction['buy_amount'] = amount_bought
            transaction['sell_amount'] = amount_sold
            ##PROCESS TRANSACTIOn
            process_order(transaction)

        elif order.buy_amount > partyOne.sell_amount:
            #ID
            transaction['creator_id'] = order.id
            #Sending
            transaction['receiver_pk'] = order.receiver_pk
            transaction['sender_pk'] = order.sender_pk

            transaction['buy_currency'] = order.buy_currency
            transaction['sell_currency'] = order.sell_currency

            #Buy Amount
            s_amount_bought = order.buy_amount - partyOne.sell_amount
            s_amount_sold = order.sell_amount - partyOne.buy_amount
            #Transaction
            transaction['buy_amount'] = s_amount_bought
            transaction['sell_amount'] = s_amount_sold
            #Process
            process_order(transaction)
