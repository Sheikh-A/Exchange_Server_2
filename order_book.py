from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    order_obj = Order( sender_pk=order['sender_pk'],receiver_pk=order['receiver_pk'], buy_currency=order['buy_currency'], sell_currency=order['sell_currency'], buy_amount=order['buy_amount'], sell_amount=order['sell_amount'] )
    if 'creator_id' in order.keys():
        order_obj.creator_id = order['creator_id']
    order = order_obj
    session.add(order)
    session.commit()

    orders = session.query(Order).filter(Order.filled == None).filter(Order.buy_currency == order.sell_currency).filter(Order.sell_currency == order.buy_currency).all()
    id = -1
    rate = 0

    for candidate in orders:
        new_rate = candidate.sell_amount / candidate.buy_amount
        if new_rate > rate:
            rate = new_rate
            id = candidate.id

    if id != -1 and rate >= order.buy_amount / order.sell_amount:
        other = session.query(Order).get(id)
        order = session.query(Order).get(order.id)
        filled = datetime.now()
        order.filled = filled
        other.filled = filled
        order.counterparty_id = id
        other.counterparty_id = order.id
        session.commit()
        new_order = {}
        new_Order = None
        if order.buy_amount > other.sell_amount:
            new_order['sender_pk'] = order.sender_pk
            new_order['receiver_pk'] = order.receiver_pk
            new_order['buy_currency'] = order.buy_currency
            new_order['sell_currency'] = order.sell_currency
            new_order['buy_amount'] = order.buy_amount - other.sell_amount
            new_order['sell_amount'] = order.sell_amount - other.buy_amount
            new_order['creator_id'] = order.id
            process_order(new_order)
        elif other.buy_amount > order.sell_amount:
            new_order['sender_pk'] = other.sender_pk
            new_order['receiver_pk'] = other.receiver_pk
            new_order['buy_currency'] = other.buy_currency
            new_order['sell_currency'] = other.sell_currency
            new_order['buy_amount'] = other.buy_amount - order.sell_amount
            new_order['sell_amount'] = other.sell_amount - order.buy_amount
            new_order['creator_id'] = other.id
            process_order(new_order)
