from rest_framework.permissions import IsAuthenticated
from rest_framework import filters, status, viewsets
from rest_framework.response import Response

from .models import Shop, APIKey, Selfbuy
from .serlializers import ShopSerializer, SelfbuySerializer, APIKeySerializer


class ShopViewSet(viewsets.ModelViewSet):
    serializer_class = ShopSerializer
    permission_classes = (IsAuthenticated,)
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ["shop_token", "name", "is_active"]
    ordering = ["-is_active", "name"]

    def get_queryset(self):
        return Shop.objects.all()

    def get_object(self):
        lookup_field_value = self.kwargs[self.lookup_field]

        obj = Shop.objects.get(id=lookup_field_value)
        self.check_object_permissions(self.request, obj)

        return obj

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        if request.user.is_authenticated:
            serializer.user = request.user
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(serializer.data, status=status.HTTP_201_CREATED)


class APIKeyViewSet(viewsets.ModelViewSet):
    serializer_class = APIKeySerializer
    permission_classes = (IsAuthenticated,)
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ["type", "name", "is_active"]
    ordering = ["-is_active", "name"]

    def get_queryset(self):
        return APIKey.objects.all()

    def get_object(self):
        lookup_field_value = self.kwargs[self.lookup_field]

        obj = APIKey.objects.get(id=lookup_field_value)
        self.check_object_permissions(self.request, obj)

        return obj

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(serializer.data, status=status.HTTP_201_CREATED)


class SelfbuyViewSet(viewsets.ModelViewSet):
    serializer_class = SelfbuySerializer
    permission_classes = (IsAuthenticated,)
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ["order", "dt_buy", "name"]
    ordering = ["-dt_buy", "order"]

    def get_queryset(self):
        return Selfbuy.objects.all()

    def get_object(self):
        lookup_field_value = self.kwargs[self.lookup_field]

        obj = Selfbuy.objects.get(id=lookup_field_value)
        self.check_object_permissions(self.request, obj)

        return obj

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(serializer.data, status=status.HTTP_201_CREATED)
