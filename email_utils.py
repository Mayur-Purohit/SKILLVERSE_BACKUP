"""
Email Utility Module for SkillVerse

This module handles all email sending functionality using Flask-Mail.
Provides utility functions to send HTML emails for various events.
"""

from flask import render_template, current_app
from flask_mail import Mail, Message
from threading import Thread

mail = Mail()


def send_async_email(app, msg):
    """
    Send email asynchronously in a separate thread
    
    Args:
        app: Flask application instance
        msg: Flask-Mail Message object
    """
    with app.app_context():
        try:
            mail.send(msg)
        except Exception as e:
            current_app.logger.error(f"Failed to send email: {str(e)}")


def send_email(subject, recipient, template, **kwargs):
    """
    Send HTML email using template
    
    Args:
        subject (str): Email subject line
        recipient (str): Recipient email address
        template (str): Template file name (without .html extension)
        **kwargs: Additional context variables for the template
        
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        app = current_app._get_current_object()
        
        msg = Message(
            subject=subject,
            recipients=[recipient],
            sender=app.config['MAIL_DEFAULT_SENDER']
        )
        
        # Render HTML template
        msg.html = render_template(f'emails/{template}.html', **kwargs)
        
        # Send asynchronously to avoid blocking
        Thread(target=send_async_email, args=(app, msg)).start()
        
        return True
    except Exception as e:
        current_app.logger.error(f"Error creating email: {str(e)}")
        return False


def send_welcome_email(user):
    """
    Send welcome email to new user
    
    Args:
        user: User object
    """
    return send_email(
        subject='Welcome to SkillVerse',
        recipient=user.email,
        template='welcome',
        user=user
    )


def send_order_placed_emails(order):
    """
    Send order placement confirmation emails to both customer and provider
    
    Args:
        order: Order object with buyer, seller, and service relationships loaded
    """
    # Email to customer
    send_email(
        subject='Your order has been sent successfully',
        recipient=order.buyer.email,
        template='order_placed_customer',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )
    
    # Email to provider
    send_email(
        subject='New order received',
        recipient=order.seller.email,
        template='order_placed_provider',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )


def send_order_accepted_emails(order):
    """
    Send order acceptance confirmation emails to both customer and provider
    
    Args:
        order: Order object
    """
    # Email to customer
    send_email(
        subject='Your order has been accepted',
        recipient=order.buyer.email,
        template='order_accepted_customer',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )
    
    # Email to provider
    send_email(
        subject='Order accepted successfully',
        recipient=order.seller.email,
        template='order_accepted_provider',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )


def send_order_completed_emails(order):
    """
    Send order completion emails to both customer and provider
    
    Args:
        order: Order object
    """
    # Email to customer
    send_email(
        subject='Your order has been completed',
        recipient=order.buyer.email,
        template='order_completed_customer',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )
    
    # Email to provider
    send_email(
        subject='Order marked as completed',
        recipient=order.seller.email,
        template='order_completed_provider',
        order=order,
        customer=order.buyer,
        provider=order.seller,
        service=order.service
    )


def send_booking_confirmation_email(booking):
    """
    Send booking confirmation email to customer
    """
    from flask import url_for
    
    start_time = booking.slot.start_time.strftime('%Y-%m-%d %H:%M UTC') # Ideally convert to user timezone
    service_title = booking.service.title if booking.service else 'General Session'
    link = url_for('user.bookings_list', _external=True)
    
    send_email(
        subject='Booking Confirmed!',
        recipient=booking.client.email,
        template='booking_confirmation',
        customer=booking.client,
        provider=booking.slot.provider,
        start_time=start_time,
        service_title=service_title,
        order_id=booking.order_id,
        link=link
    )

def send_booking_rejection_email(booking):
    """
    Send booking rejection email to customer
    """
    from flask import url_for
    
    start_time = booking.slot.start_time.strftime('%Y-%m-%d %H:%M UTC')
    service_title = booking.service.title if booking.service else 'General Session'
    
    # Link to order detail to reschedule if possible, else service page
    if booking.order_id:
        link = url_for('user.order_detail', order_id=booking.order_id, _external=True)
    elif booking.service_id:
        link = url_for('service.detail', service_id=booking.service_id, _external=True)
    else:
        link = url_for('service.browse', _external=True)
    
    send_email(
        subject='Action Required: Booking Request Rejected',
        recipient=booking.client.email,
        template='booking_rejection',
        customer=booking.client,
        provider=booking.slot.provider,
        start_time=start_time,
        service_title=service_title,
        link=link
    )
