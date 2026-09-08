FROM nginx:alpine

# Copy all static single page application assets directly to Nginx content root
COPY . /usr/share/nginx/html

# Expose standard HTTP port
EXPOSE 80

# Run nginx in foreground
CMD ["nginx", "-g", "daemon off;"]
