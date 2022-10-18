from api.models import Daily
from . import execute_sql


class Update_Daily(object):
    @staticmethod
    def stocks(params=None):
        params = params or []
        sql = f"""
        INSERT INTO {Daily.objects.model._meta.db_table}
        (date, sku, stocks, shop_id)
        (
            SELECT 
                mos.date,
                moso.sku,
                --mos.type,
                --mos.product_id,
                mos.present as stocks, 
                mos.shop_id
            FROM mp_ozon_stock mos
            INNER JOIN mp_ozon_sku_offer moso ON mos.product_id = moso.product_id AND mos.type = moso.type
                AND moso.sku > 0
            WHERE mos.shop_id = %(shop_id)s
        ) ON CONFLICT (shop_id, date, sku) DO UPDATE SET 
            stocks = excluded.stocks;
        """

        return execute_sql(sql, params=params)

    @staticmethod
    def transactions(params=None):
        params = params or []
        sql = f"""
        INSERT INTO {Daily.objects.model._meta.db_table}
        (date, sku, premium, rassrochka, shop_id)
        (
            SELECT 
                mot.operation_date,
                mot.sku, 
                SUM(case when operation_type = 'OperationMarketplaceServicePremiumCashback' then mot.amount else 0 end) as premium, 
                SUM(case when operation_type = 'MarketplaceSellerInstallmentOperation' then mot.amount else 0 end) as rassrochka,
                mot.shop_id
            FROM mp_ozon_transaction mot
            WHERE operation_type IN ('MarketplaceSellerInstallmentOperation', 'OperationMarketplaceServicePremiumCashback')
            AND shop_id = %(shop_id)s
            GROUP BY mot.operation_date, mot.sku, mot.shop_id
        ) ON CONFLICT (shop_id, date, sku) DO UPDATE SET 
            premium = excluded.premium,
            rassrochka = excluded.rassrochka;
        """

        return execute_sql(sql, params=params)

    @staticmethod
    def orders(params=None):
        params = params or []
        sql = f"""
        INSERT INTO {Daily.objects.model._meta.db_table}
        (date, sku, selfbuy_cnt, selfbuy_amount, shop_id)
        (
            SELECT 
                date(mof.created_at) as order_at,
                mofp.sku,  
                SUM(mofp.quantity) as selfbuy_cnt, 
                SUM(mofp.quantity * mofp.price) as selfbuy_amount,
                mof.shop_id
            FROM mp_ozon_fbo mof
            INNER JOIN mp_ozon_fbo_product mofp ON mof.id = mofp.order_id
            INNER JOIN mp_selfbuy ms ON ms.shop_id = mof.shop_id AND (mof.order_number = ms.order OR mof.posting_number = ms.order)
            WHERE mof.shop_id = %(shop_id)s
            GROUP BY date(mof.created_at), mofp.sku, mof.shop_id
        ) ON CONFLICT (shop_id, date, sku) DO UPDATE SET 
            selfbuy_cnt = excluded.selfbuy_cnt,
            selfbuy_amount = excluded.selfbuy_amount;
        """

        return execute_sql(sql, params=params)

    @staticmethod
    def campaigns(params=None):
        params = params or []
        sql = f"""
            INSERT INTO {Daily.objects.model._meta.db_table}
            (date, sku, adv_promo_bid, adv_promo_visibility, shop_id)
            (
                SELECT 
                    moch.date, 
                    moso.sku, 
                    MAX(bid) as bid, 
                    MIN(visibility_idx) as visibility_idx,                    
                    moc.shop_id
                FROM mp_ozon_campaignproduct_history moch
                INNER JOIN mp_ozon_sku_offer moso ON moch.product_id = moso.product_id
                INNER JOIN mp_ozon_campaign moc ON moch.campaign_id = moc.id 
                WHERE moc.shop_id = %(shop_id)s
                GROUP BY date, moso.sku, moc.shop_id
            ) ON CONFLICT (shop_id, date, sku) DO UPDATE SET 
                adv_promo_bid = excluded.adv_promo_bid,
                adv_promo_visibility = excluded.adv_promo_visibility;
        """

        return execute_sql(sql, params=params)
