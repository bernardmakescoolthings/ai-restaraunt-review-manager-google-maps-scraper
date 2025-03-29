FROM postgres:17

# Set environment variables
ENV POSTGRES_DB=googlemaps
ENV POSTGRES_USER=reviewsuser
ENV POSTGRES_PASSWORD=reviewspass

# Expose the PostgreSQL port
EXPOSE 5432

# The default command will start PostgreSQL
CMD ["postgres"] 