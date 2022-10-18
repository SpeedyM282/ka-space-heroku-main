from rest_framework.routers import SimpleRouter

from .viewsets import ShopViewSet, APIKeyViewSet, SelfbuyViewSet


routes = SimpleRouter()

routes.register(r"shop", ShopViewSet, basename="shop")
routes.register(r"apikey", APIKeyViewSet, basename="apikey")
routes.register(r"selfbuy", SelfbuyViewSet, basename="selfbuy")
