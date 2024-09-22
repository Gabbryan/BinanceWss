from functools import wraps

from .slack_channel import SlackChannel


class SlackDecorators:
    def __init__(self, slack_channel: SlackChannel):
        self.slack_channel = slack_channel

    def notify(self, header: str, message: str, color: str = "#6a0dad"):
        """
        Décorateur pour envoyer un message après l'exécution d'une fonction.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                self.slack_channel.send_message(header, message, color)
                return result

            return wrapper

        return decorator

    def notify_with_link(self, header: str, message: str, color: str = "#6a0dad"):
        """
        Décorateur pour envoyer un message avec un lien après l'exécution d'une fonction.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                link = result  # Suppose que la fonction retourne le lien
                self.slack_channel.send_message(header, message, color, link)
                return result

            return wrapper

        return decorator

    def notify_with_result(self, header: str, color: str = "#6a0dad"):
        """
        Décorateur pour envoyer le retour de la fonction comme message après l'exécution.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                self.slack_channel.send_message(header, str(result), color)
                return result

            return wrapper

        return decorator

    def notify_with_files(self, header: str, color: str = "#6a0dad"):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result, symbol, start_date, end_date = func(*args, **kwargs)
                if result:
                    message = (
                        f"*Processing Complete for {symbol}* :white_check_mark:\n"
                        f"Date Range: *{start_date}* to *{end_date}*\n"
                        "Here are the exported files:\n"
                    )
                    for file_info in result:
                        message += f"- <{file_info['url']}|{file_info['url']}> (Size: *{file_info['size'] / (1024 * 1024):.2f} MB*)\n"
                    self.slack_channel.send_message(header, message, color)
                return result

            return wrapper

        return decorator
