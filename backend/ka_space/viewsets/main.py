from rest_framework import status
from rest_framework import views
from rest_framework.response import Response


class MainView(views.APIView):
    def get(self, request):
        result = {
            "/": "This page",
            "/api/auth/login": "Authorization",
            "/api/auth/registration": "Registration",
            "/api/auth/refresh": "Token refresh",
            "GET: /api/shop/": "List of shops",
            "POST: /api/shop/": "New shop",
            "GET: /api/shop/<int>/": "View shop",
            "PUT: /api/shop/<int>/": "Update shop",
            "DELETE: /api/shop/<int>/": "Delete shop",
        }
        return Response(result, status=status.HTTP_200_OK)
