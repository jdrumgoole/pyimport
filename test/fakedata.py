# from mimesis import Person, Address, Internet
#
#
# def generate_user_profiles(count, seed=None):
#     """Generates a specified number of fake user profiles as a generator.
#
#     Args:
#       count: The number of profiles to generate.
#       seed: An optional integer to seed the random number generator for repeatable data.
#
#     Yields:
#       A dictionary containing user information.
#     """
#     # Create Mimesis objects with optional seeding
#     person = Person(seed=seed)
#     address = Address(seed=seed)
#     internet = Internet(seed=seed)
#
#     for _ in range(count):
#         # Generate user data
#         yield {
#             "full_name": person.full_name(),
#             "email": internet.email(domain="example.com"),
#             "username": internet.username(),
#             "phone_number": person.telephone(),
#             "street": address.street(),
#             "city": address.city(),
#             "country": address.country()
#         }
#
#
# # Example usage with and without seed
# user_count = 5
# for profile in generate_user_profiles(user_count):
#     print(profile)
#
# print("\nGenerating again with same seed for comparison:")
# for profile in generate_user_profiles(user_count, seed=42):
#     print(profile)
from datetime import datetime

from mimesis import Person, Cryptographic
from mimesis.locales import Locale
from mimesis.schema import Schema
import random


def generate_user_profiles(count: int, seed: int = None):
    """
    Generate random user profiles using mimesis.

    Args:
        count (int): Number of profiles to generate.
        seed (int, optional): Seed for random number generator. Defaults to None.

    Yields:
        dict: A dictionary representing a user profile.
    """

    crypto = Cryptographic(seed=seed)
    person = Person(locale=Locale.EN, seed=seed)

    for _ in range(count):
        yield {
            "id": crypto.uuid(),
            "name": person.full_name(),
            "email": person.email(),
            "username": person.username(),
            "DOB": datetime.combine(person.birthdate(), datetime.min.time()),  # mongodb needs datetime objects
            "phone": person.telephone(),
            "gender": person.gender(),
        }


# Example usage:
if __name__ == "__main__":
    for profile in generate_user_profiles(5, seed=42):
        print(profile)
