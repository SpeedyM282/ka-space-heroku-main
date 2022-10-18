from rest_framework.routers import SimpleRouter
from ka_space.user.viewsets import UserViewSet
from ka_space.auth.viewsets import LoginViewSet, RegistrationViewSet, RefreshViewSet

from mp.routers import routes as mp_routes

routes = SimpleRouter()

# AUTHENTICATION
routes.register(r"auth/login", LoginViewSet, basename="auth-login")
routes.register(r"auth/register", RegistrationViewSet, basename="auth-register")
routes.register(r"auth/refresh", RefreshViewSet, basename="auth-refresh")

# USER
routes.register(r"user", UserViewSet, basename="user")

urlpatterns = [*routes.urls, *mp_routes.urls]
